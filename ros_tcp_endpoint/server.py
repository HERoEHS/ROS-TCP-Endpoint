#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import rclpy
import socket
import json
import sys
import threading
import importlib

from rclpy.node import Node
from rclpy.parameter import Parameter
from rclpy.executors import MultiThreadedExecutor
from rclpy.serialization import deserialize_message
from rclpy.qos import QoSProfile, QoSDurabilityPolicy, QoSReliabilityPolicy
from std_msgs.msg import Bool, String
from std_srvs.srv import SetBool

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService


class TcpServer(Node):
    """
    Initializes ROS node and TCP server.
    """

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        super().__init__(node_name)

        self.declare_parameter("ROS_IP", "0.0.0.0")
        self.declare_parameter("ROS_TCP_PORT", 10000)
        self.declare_parameter("topic_policy_config", "")

        # ── 링크 강건화 파라미터 ──────────────────────────────────────────────
        # 무선(로봇 ↔ 조종 단말) 링크에서 연결이 조용히 죽거나 지연이 누적되는 것을 막는다.
        #   TCP_NODELAY  : Nagle 비활성. 조이스틱 명령처럼 작고 잦은 패킷이 최대 40ms 뭉치는 것 방지.
        #   TCP_KEEPALIVE: 죽은 링크(AP 로밍/전원 차단/Wi-Fi 단절)를 keepalive 프로브로 감지해
        #                  커널이 소켓을 끊어준다. 미설정 시 서버 recv 가 무한 대기하며 좀비
        #                  연결이 남고, 재접속하면 클라이언트가 둘로 보인다.
        #                  IDLE + INTVL×CNT ≈ 감지 시간(기본 2 + 1×3 = 5초).
        #   SINGLE_CLIENT: 새 연결을 받으면 이전 연결을 정리한다. 조종 단말은 하나이므로
        #                  재접속 시 좀비 세션이 남아 송신 큐가 갈라지는 것을 막는다.
        # (송신 큐 상한은 topic_policy.yaml 의 default_policy.max_queue_size 가 담당한다 —
        #  별도 파라미터를 두지 않고 기존 정책 설정을 그대로 쓴다.)
        self.declare_parameter("TCP_NODELAY", True)
        self.declare_parameter("TCP_KEEPALIVE", True)
        self.declare_parameter("TCP_KEEPIDLE", 2)     # 유휴 후 첫 프로브까지 [s]
        self.declare_parameter("TCP_KEEPINTVL", 1)    # 프로브 간격 [s]
        self.declare_parameter("TCP_KEEPCNT", 3)      # 무응답 허용 프로브 수
        self.declare_parameter("SINGLE_CLIENT", True)

        if tcp_ip:
            self.loginfo("Using ROS_IP override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = self.get_parameter("ROS_IP").get_parameter_value().string_value

        if tcp_port:
            self.loginfo("Using ROS_TCP_PORT override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = self.get_parameter("ROS_TCP_PORT").get_parameter_value().integer_value

        # 토픽 정책 설정 파일 경로 (launch 파라미터로 지정 가능)
        config_path = self.get_parameter("topic_policy_config").get_parameter_value().string_value
        config_path = config_path if config_path else None

        self.tcp_nodelay = self.get_parameter("TCP_NODELAY").get_parameter_value().bool_value
        self.tcp_keepalive = self.get_parameter("TCP_KEEPALIVE").get_parameter_value().bool_value
        self.tcp_keepidle = self.get_parameter("TCP_KEEPIDLE").get_parameter_value().integer_value
        self.tcp_keepintvl = self.get_parameter("TCP_KEEPINTVL").get_parameter_value().integer_value
        self.tcp_keepcnt = self.get_parameter("TCP_KEEPCNT").get_parameter_value().integer_value
        self.single_client = self.get_parameter("SINGLE_CLIENT").get_parameter_value().bool_value

        self.unity_tcp_sender = UnityTcpSender(self, config_path=config_path)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        self.executor = None

        # Graceful shutdown 지원
        self.server_socket = None
        self.shutdown_event = threading.Event()
        self.client_threads = []
        self.client_threads_lock = threading.Lock()

        # TCP 통신 토글: 노드 시작 시 자동으로 ON(start 에서 set_tcp_enabled(True)).
        # 이후 외부(gui SetBool)로 끄고 켤 수 있다.
        self._tcp_enabled = threading.Event()
        self._socket_lock = threading.Lock()
        self._client_lock = threading.Lock()
        self._client_sockets = {}
        self._server_thread = None

        status_qos = QoSProfile(
            depth=1,
            reliability=QoSReliabilityPolicy.RELIABLE,
            durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
        )
        self._enabled_pub = self.create_publisher(Bool, "/ros_tcp_endpoint/enabled", status_qos)
        self._status_pub = self.create_publisher(String, "/ros_tcp_endpoint/status", status_qos)
        # 클라이언트 접속 유무만 담은 경량 신호(True=1개 이상 접속 중).
        # status(String JSON)를 파싱하지 않고도 링크 단절을 감지할 수 있게 한다 —
        # AAM RlModule 이 이 토픽의 True→False 엣지를 외부 조종(TCP 조이스틱) 링크
        # 단절로 보고 RL 보행 안전 시퀀스(감속 → 제자리걸음 → stand)를 즉시 개시한다.
        # (transient_local: 늦게 붙은 구독자도 마지막 상태를 즉시 받는다. 최초 latch 값은
        #  False 이므로 구독 측은 "True 를 한 번이라도 본 뒤의 False" 만 단절로 판정할 것.)
        self._connected_pub = self.create_publisher(Bool, "/ros_tcp_endpoint/connected", status_qos)
        self._enabled_service = self.create_service(
            SetBool, "/ros_tcp_endpoint/set_enabled", self._set_enabled_callback
        )


    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        self._publish_enabled(False)
        self._server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        self._server_thread.daemon = True
        self._server_thread.start()
        # 노드 시작 시 항상 TCP 를 켠다. 실패해도 노드는 계속 살아 있다(gui 로 재시도 가능).
        self.set_tcp_enabled(True)

    # ─── TCP toggle (gui SetBool / mobile 자동으로 on/off) ──────────────────────
    @property
    def tcp_enabled(self):
        return self._tcp_enabled.is_set()

    def _publish_enabled(self, enabled):
        if not rclpy.ok():
            return
        msg = Bool()
        msg.data = bool(enabled)
        self._enabled_pub.publish(msg)
        self._publish_status()

    def _publish_status(self):
        if not rclpy.ok():
            return
        with self._client_lock:
            clients = sorted(
                self._client_sockets.values(), key=lambda item: (item["ip"], item["port"])
            )
        msg = String()
        msg.data = json.dumps({
            "enabled": self.tcp_enabled,
            "listen_ip": self.tcp_ip,
            "listen_port": self.tcp_port,
            "clients": clients,
        })
        self._status_pub.publish(msg)

        # 접속 유무 경량 신호. 상태가 바뀌는 모든 지점(enable/disable, accept,
        # unregister_client)이 이 함수를 거치므로 여기 한 곳에서만 발행하면 된다.
        connected = Bool()
        connected.data = bool(clients)
        self._connected_pub.publish(connected)

    def _open_server_socket(self):
        """ON 전환 시점에만 외부 TCP 포트를 bind/listen 한다."""
        with self._socket_lock:
            if self.server_socket is not None:
                return
            tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                tcp_server.bind((self.tcp_ip, self.tcp_port))
                tcp_server.listen(self.connections)
                tcp_server.settimeout(0.25)
            except Exception:
                tcp_server.close()
                raise
            self.server_socket = tcp_server

    def _close_server_socket(self):
        with self._socket_lock:
            tcp_server = self.server_socket
            self.server_socket = None
        if tcp_server is not None:
            try:
                tcp_server.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            tcp_server.close()

    def _configure_client_socket(self, conn):
        """accept 된 연결에 지연/단절 대응 소켓 옵션을 적용한다.

        플랫폼에 없는 옵션(TCP_KEEPIDLE 등은 Linux 전용)은 조용히 건너뛴다.
        옵션 설정 실패가 연결 자체를 막아서는 안 되므로 예외는 경고로만 남긴다.
        """
        try:
            if self.tcp_nodelay:
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.tcp_keepalive:
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                for opt_name, value in (
                    ("TCP_KEEPIDLE", max(1, self.tcp_keepidle)),
                    ("TCP_KEEPINTVL", max(1, self.tcp_keepintvl)),
                    ("TCP_KEEPCNT", max(1, self.tcp_keepcnt)),
                ):
                    opt = getattr(socket, opt_name, None)
                    if opt is not None:
                        conn.setsockopt(socket.IPPROTO_TCP, opt, value)
        except OSError as exc:
            self.logwarn("Failed to apply socket options: {}".format(exc))

    def _drop_other_clients(self, keep_conn):
        """SINGLE_CLIENT 모드: 같은 단말(동일 IP)의 이전 연결만 정리한다.

        조종 단말이 재접속했는데 이전 세션이 좀비로 남아 있으면 송신 큐가 죽은 소켓 쪽으로
        갈라지고(마지막 sender 가 self.queue 를 차지) status/connected 신호도 실제와 어긋난다.

        단, vr(HMD)·joystick 처럼 서로 다른 단말이 각기 다른 IP 로 동시에 붙을 수 있으므로
        '모든' 다른 연결이 아니라 keep_conn 과 같은 IP 의 이전 연결만 끊는다. 다른 IP 를 끊으면
        단말끼리 서로 밀어내며(accept 마다 상대를 종료 → 상대가 재접속 → 다시 종료) 접속이
        무한히 깜빡인다. 재접속 좀비 세션 정리라는 원 목적은 동일 IP 만 대상으로도 달성된다.
        """
        with self._client_lock:
            keep_info = self._client_sockets.get(keep_conn)
            keep_ip = keep_info["ip"] if keep_info else None
            stale = [
                c for c, info in self._client_sockets.items()
                if c is not keep_conn and keep_ip is not None and info["ip"] == keep_ip
            ]
            for conn in stale:
                self._client_sockets.pop(conn, None)
        for conn in stale:
            self.loginfo("Dropping stale connection (new client took over)")
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                conn.close()
            except OSError:
                pass

    def _disconnect_clients(self):
        with self._client_lock:
            conns = list(self._client_sockets.keys())
            self._client_sockets.clear()
        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def unregister_client(self, conn):
        with self._client_lock:
            removed = self._client_sockets.pop(conn, None)
        if removed is not None:
            self._publish_status()

    def set_tcp_enabled(self, enabled):
        """TCP 리스닝을 켜고/끈다. OFF 전환 시 모든 클라이언트를 끊는다."""
        enabled = bool(enabled)
        if enabled == self.tcp_enabled:
            if not enabled:
                self._close_server_socket()
                self._disconnect_clients()
            self._publish_enabled(enabled)
            return True, "TCP endpoint is already {}".format("ON" if enabled else "OFF")

        if enabled:
            try:
                self._open_server_socket()
            except OSError as exc:
                self.logerr("Failed to enable TCP endpoint: {}".format(exc))
                self._publish_enabled(False)
                return False, "Failed to open {}:{}: {}".format(self.tcp_ip, self.tcp_port, exc)
            self._tcp_enabled.set()
            self._publish_enabled(True)
            self.loginfo("TCP communication enabled on {}:{}".format(self.tcp_ip, self.tcp_port))
            return True, "TCP endpoint enabled"

        # 이벤트를 먼저 내린 뒤 소켓을 닫아 accept 직후의 연결도 차단한다.
        self._tcp_enabled.clear()
        self._close_server_socket()
        self._disconnect_clients()
        self._publish_enabled(False)
        self.loginfo("TCP communication disabled")
        return True, "TCP endpoint disabled"

    def _set_enabled_callback(self, request, response):
        response.success, response.message = self.set_tcp_enabled(request.data)
        return response

    def listen_loop(self):
        """
            _tcp_enabled 가 켜질 때까지 대기하다가, 켜지면 set_tcp_enabled 가 연 server_socket 으로
            연결을 accept 한다. 각 연결마다 ClientThread 를 만든다.
        """
        self.loginfo("TCP endpoint ready; waiting for enable (/ros_tcp_endpoint/set_enabled)")
        while not self.shutdown_event.is_set():
            if not self._tcp_enabled.wait(timeout=0.25):
                continue
            with self._socket_lock:
                tcp_server = self.server_socket
            if tcp_server is None:
                continue
            try:
                (conn, (ip, port)) = tcp_server.accept()
                reject_connection = False
                with self._client_lock:
                    # OFF 전환과 같은 lock 으로 직렬화해 accept 직후 연결 누수를 막는다.
                    if not self.tcp_enabled:
                        reject_connection = True
                    else:
                        self._client_sockets[conn] = {"ip": ip, "port": port}
                if reject_connection:
                    conn.close()
                    continue
                # 소켓 최적화: 지연 최소화 및 죽은 링크 감지.
                #   SO_KEEPALIVE 만 켜면 리눅스 기본 유휴시간(7200s) 뒤에야 프로브가 나가
                #   실질적으로 감지되지 않는다 → KEEPIDLE/INTVL/CNT 까지 함께 설정한다.
                self._configure_client_socket(conn)
                if self.single_client:
                    self._drop_other_clients(conn)
                self._publish_status()
                client_thread = ClientThread(conn, self, ip, port)
                with self.client_threads_lock:
                    self.client_threads.append(client_thread)
                client_thread.start()
            except socket.timeout:
                continue
            except OSError as e:
                # OFF 전환/종료 시 accept 중인 소켓을 닫으면 정상적으로 발생한다.
                if self.tcp_enabled and not self.shutdown_event.is_set():
                    self.logerr("TCP accept failed: {}".format(e))

    def send_unity_error(self, error):
        self.unity_tcp_sender.send_unity_error(error)

    def send_unity_message(self, topic, message):
        self.unity_tcp_sender.send_unity_message(topic, message)

    def send_unity_service(self, topic, service_class, request):
        return self.unity_tcp_sender.send_unity_service_request(topic, service_class, request)

    def send_unity_service_response(self, srv_id, data):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data)

    def handle_syscommand(self, topic, data, client=None):
        function = getattr(self.syscommands, topic[2:], None)
        if function is None:
            self.send_unity_error("Don't understand SysCommand.'{}'".format(topic))
            return

        try:
            message_json = data.decode("utf-8")[:-1]
            params = json.loads(message_json)
            # 서비스 요청/응답 명령은 클라이언트 참조 필요
            self.syscommands.current_client = client
            function(**params)
            self.syscommands.current_client = None
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            self.send_unity_error("Invalid syscommand data for '{}': {}".format(topic, e))
            self.logerr("Failed to parse syscommand '{}': {}".format(topic, e))

    def loginfo(self, text):
        self.get_logger().info(text)

    def logwarn(self, text):
        self.get_logger().warning(text)

    def logerr(self, text):
        self.get_logger().error(text)

    def setup_executor(self):
        """
            Since rclpy.spin() is a blocking call the server needed a way
            to spin all of the relevant nodes at the same time.

            MultiThreadedExecutor allows us to set the number of threads
            needed as well as the nodes that need to be spun.
        """
        num_threads = (
            len(self.publishers_table.keys())
            + len(self.subscribers_table.keys())
            + len(self.ros_services_table.keys())
            + len(self.unity_services_table.keys())
            + 1
        )
        executor = MultiThreadedExecutor(num_threads)

        executor.add_node(self)

        for ros_node in self.publishers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.subscribers_table.values():
            executor.add_node(ros_node)
        for ros_node in self.ros_services_table.values():
            executor.add_node(ros_node)
        for ros_node in self.unity_services_table.values():
            executor.add_node(ros_node)

        self.executor = executor
        executor.spin()

    def unregister_node(self, old_node):
        """
        Safely unregister a node from the executor.
        Handles race condition where executor might still be processing callbacks.
        """
        if old_node is None:
            return

        import time

        # 1. 먼저 executor에서 제거 (spin이 더 이상 이 노드를 처리하지 않도록)
        if self.executor is not None:
            try:
                self.executor.remove_node(old_node)
            except ValueError:
                # 이미 제거된 경우
                pass
            except Exception as e:
                self.logerr(f"Error removing node from executor: {e}")

        # 2. 진행 중인 콜백이 완료되도록 약간의 지연
        time.sleep(0.05)

        # 3. subscription 정리 (unregister 호출)
        try:
            old_node.unregister()
        except Exception as e:
            self.logerr(f"Error unregistering node: {e}")

        # 4. 추가 지연 후 노드 파괴
        time.sleep(0.05)

        try:
            old_node.destroy_node()
        except Exception as e:
            self.logerr(f"Error destroying node: {e}")

    def stop(self):
        """
        Graceful shutdown - 리스닝 종료 + 서버 소켓/클라이언트 연결 정리 + 스레드 join.
        (기존 shutdown() 을 toggle 소켓 정리와 통합한 것.)
        """
        self.loginfo("Initiating graceful shutdown...")
        self.shutdown_event.set()
        self._tcp_enabled.clear()
        self._close_server_socket()
        self._disconnect_clients()
        self._publish_enabled(False)

        # listen_loop 스레드 종료 대기
        if self._server_thread is not None:
            self._server_thread.join(timeout=1.0)

        # 클라이언트 스레드들이 종료되길 대기
        with self.client_threads_lock:
            threads = list(self.client_threads)
        for thread in threads:
            thread.join(timeout=1.0)

        self.loginfo("Graceful shutdown complete")

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
        # Graceful shutdown 먼저 수행
        self.stop()

        for ros_node in self.publishers_table.values():
            ros_node.destroy_node()
        for ros_node in self.subscribers_table.values():
            ros_node.destroy_node()
        for ros_node in self.ros_services_table.values():
            ros_node.destroy_node()
        for ros_node in self.unity_services_table.values():
            ros_node.destroy_node()

        self.destroy_node()


class SysCommands:
    def __init__(self, tcp_server):
        self.tcp_server = tcp_server
        self.current_client = None  # 현재 요청을 처리 중인 클라이언트

    def subscribe(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.subscribers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_subscriber = RosSubscriber(topic, message_class, self.tcp_server)
        self.tcp_server.subscribers_table[topic] = new_subscriber
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_subscriber)

        self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def publish(self, topic, message_name, queue_size=10, latch=False):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.publishers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_publisher = RosPublisher(topic, message_class, queue_size=queue_size, latch=latch)

        self.tcp_server.publishers_table[topic] = new_publisher
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_publisher)

        self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.ros_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = RosService(topic, message_class)

        self.tcp_server.ros_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.unity_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = UnityService(str(topic), message_class, self.tcp_server)

        self.tcp_server.unity_services_table[topic] = new_service
        if self.tcp_server.executor is not None:
            self.tcp_server.executor.add_node(new_service)

        self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def response(self, srv_id):  # the next message is a service response
        if self.current_client is not None:
            self.current_client.pending_srv_id = srv_id
            self.current_client.pending_srv_is_request = False

    def request(self, srv_id):  # the next message is a service request
        if self.current_client is not None:
            self.current_client.pending_srv_id = srv_id
            self.current_client.pending_srv_is_request = True

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list()

    def resolve_message_name(self, name, extension="msg"):
        try:
            names = name.split("/")
            module_name = names[0]
            class_name = names[1]
            importlib.import_module(module_name + "." + extension)
            module = sys.modules.get(module_name)
            if module is None:
                self.tcp_server.logerr("Failed to resolve module {}".format(module_name))
                return None
            # getattr은 속성 없으면 AttributeError 발생 (except에서 처리)
            module = getattr(module, extension)
            message_class = getattr(module, class_name)
            return message_class
        except (IndexError, KeyError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None

    def remove_subscriber(self, topic):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't unsubscribe to a blank topic name! SysCommand.remove_subscriber({})".format(
                    topic
                )
            )
            return

        node = self.tcp_server.subscribers_table.get(topic)
        if node is not None:
            # 먼저 테이블에서 제거
            del self.tcp_server.subscribers_table[topic]

            # 그 다음 노드 정리
            self.tcp_server.unregister_node(node)
            self.tcp_server.loginfo("UnregisterSubscriber({}) OK".format(topic))
        else:
            self.tcp_server.logwarn(
                "Topic '{}' was not subscribed, ignoring remove_subscriber request.".format(
                    topic
                )
            )
