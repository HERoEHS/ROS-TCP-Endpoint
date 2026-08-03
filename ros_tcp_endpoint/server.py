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
import os
import re
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

        # TCP 통신 토글: 기본은 OFF(포트를 열지 않음). set_tcp_enabled(True) 시에만 bind/accept.
        # 노드/서비스는 항상 살아 있고, 외부(gui SetBool) 또는 mobile 자동 로직이 켠다.
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
        self._enabled_service = self.create_service(
            SetBool, "/ros_tcp_endpoint/set_enabled", self._set_enabled_callback
        )

        # mobile(alice_m{N}) 세대는 gui 없이 코드가 자동으로 TCP 를 켠다(start 에서 set_tcp_enabled).
        # 휴머노이드(alice4/5)는 기본 OFF 로 두고 gui 토글로 켠다.
        self._auto_enable = bool(re.match(r"^alice_m[0-9]+$", os.environ.get("ALICE_GENERATION", "")))

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
        # mobile: 코드가 자동으로 TCP 를 켠다(gui 대신). 실패해도 노드는 계속 살아 있다.
        if self._auto_enable:
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
                # 소켓 최적화: 지연 최소화 및 연결 감지
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
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
