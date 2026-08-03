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
import time
import threading
import json
import os

from rclpy.node import Node
from rclpy.serialization import deserialize_message
from rclpy.serialization import serialize_message

from .client import ClientThread
from .thread_pauser import ThreadPauser

# queue module was renamed between python 2 and 3
try:
    from queue import Queue
    from queue import Empty
    from queue import Full
except:
    from Queue import Queue
    from Queue import Empty
    from Queue import Full

# YAML 로드 (선택적)
try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False


class UnityTcpSender:
    """
    Sends messages to Unity.
    """

    def __init__(self, tcp_server, config_path=None):
        # super().__init__(f'UnityTcpSender')

        self.sender_id = 1
        self.time_between_halt_checks = 5
        self.tcp_server = tcp_server

        # 멀티 클라이언트 지원: 클라이언트별 큐 관리
        self.queues = {}  # {client_id: Queue}
        self.queues_lock = threading.Lock()

        # variables needed for matching up unity service requests with responses
        self.next_srv_id = 1001
        self.srv_lock = threading.Lock()
        self.services_waiting = {}

        # 메모리 최적화: 토픽별 정책
        self.topic_policies = {}
        self.default_policy = {'policy': 'queue', 'max_queue_size': 100}
        self.latest_messages = {}  # {topic: serialized_message} for latest_only
        self.latest_lock = threading.Lock()
        self.last_send_time = {}  # {topic: timestamp} for throttling

        # 송신 큐 백프레셔 통계(가득 찬 큐에서 버린 토픽 메시지 수). 5초 간격으로만 로그한다.
        self.dropped_messages = 0
        self._last_drop_log_time = 0.0

        # 설정 파일 로드
        if config_path:
            self._load_config(config_path)
        else:
            # 기본 설정 파일 경로
            default_config = os.path.join(
                os.path.dirname(__file__), 'config', 'topic_policy.yaml'
            )
            if os.path.exists(default_config):
                self._load_config(default_config)

    def _load_config(self, path):
        """YAML 설정 파일 로드"""
        if not YAML_AVAILABLE:
            self.tcp_server.logwarn("PyYAML not installed, using default topic policies")
            return

        try:
            with open(path, 'r') as f:
                config = yaml.safe_load(f)
            self.topic_policies = config.get('topic_policies', {}) or {}
            self.default_policy = config.get('default_policy', self.default_policy)
            self.tcp_server.loginfo(f"Loaded topic policy config from {path}")
        except Exception as e:
            self.tcp_server.logwarn(f"Failed to load config {path}: {e}")

    def _get_policy(self, topic):
        """토픽별 정책 반환"""
        return self.topic_policies.get(topic, self.default_policy)

    def _queue_maxsize(self):
        """클라이언트 송신 큐 상한(항목 수). 0 이하 = 무제한.

        default_policy.max_queue_size 를 실제 Queue 상한으로 쓴다(종전에는 설정만 있고
        Queue() 가 무제한이라 값이 사용되지 않았다). 상한이 없으면 링크가 느려질 때 큐가
        끝없이 쌓여 (a) 조종 단말이 수 초 뒤처진 상태를 보고 (b) 메모리가 늘고
        (c) sendall 이 오래 블록해 연결이 죽은 것처럼 보인다.
        """
        try:
            return max(0, int(self.default_policy.get('max_queue_size', 0)))
        except (TypeError, ValueError):
            return 0

    def _broadcast_to_all_clients(self, data, droppable=False):
        """모든 연결된 클라이언트에게 메시지 전송.

        droppable=True (토픽 메시지): 큐가 가득 차면 가장 오래된 항목을 버리고 최신을 넣는다.
            오래된 상태 메시지는 최신 것으로 대체 가능하므로, 버리는 편이 링크를 살리는 데 유리하다.
        droppable=False (제어 메시지: 로그·서비스 응답·토픽 목록): 버리지 않되 무한 대기도 하지
            않는다. 큐에 상한이 있으면 순수 put() 은 호출 스레드(ROS 콜백 등)를 붙잡을 수 있다.
        """
        with self.queues_lock:
            queues = list(self.queues.values())

        dropped = 0
        for queue in queues:
            if not droppable:
                try:
                    queue.put(data, timeout=1.0)
                except Full:
                    self.tcp_server.logwarn("Send queue full — control message dropped")
                continue

            try:
                queue.put_nowait(data)
                continue
            except Full:
                pass
            try:
                queue.get_nowait()          # 가장 오래된 항목 폐기
            except Empty:
                pass
            try:
                queue.put_nowait(data)
            except Full:
                pass                        # 그 사이 다른 스레드가 채웠다면 이번 항목을 버린다
            dropped += 1

        if dropped:
            self.dropped_messages += dropped
            now = time.time()
            if now - self._last_drop_log_time >= 5.0:
                self._last_drop_log_time = now
                self.tcp_server.logwarn(
                    "Send queue full — dropped {} message(s) so far (link too slow "
                    "or client stalled)".format(self.dropped_messages)
                )

    def send_unity_info(self, text):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__log", command)
        self._broadcast_to_all_clients(serialized_bytes)

    def send_unity_warning(self, text):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__warn", command)
        self._broadcast_to_all_clients(serialized_bytes)

    def send_unity_error(self, text):
        command = SysCommand_Log()
        command.text = text
        serialized_bytes = ClientThread.serialize_command("__error", command)
        self._broadcast_to_all_clients(serialized_bytes)

    def send_ros_service_response(self, srv_id, destination, response):
        command = SysCommand_Service()
        command.srv_id = srv_id
        serialized_header = ClientThread.serialize_command("__response", command)
        serialized_message = ClientThread.serialize_message(destination, response)
        self._broadcast_to_all_clients(b"".join([serialized_header, serialized_message]))

    def send_unity_message(self, topic, message):
        """토픽 정책에 따라 메시지 전송"""
        policy = self._get_policy(topic)
        is_latest_only = policy.get('policy') == 'latest_only'

        serialized_message = ClientThread.serialize_message(topic, message)

        if is_latest_only:
            # latest_only: 항상 최신 값 저장 (스로틀링과 무관)
            with self.latest_lock:
                self.latest_messages[topic] = serialized_message
        else:
            # 큐 방식: 스로틀링 체크 후 브로드캐스트
            max_freq = policy.get('max_frequency', 0)
            if max_freq > 0:
                now = time.time()
                min_interval = 1.0 / max_freq
                last_time = self.last_send_time.get(topic, 0)
                if now - last_time < min_interval:
                    return  # 스킵
                self.last_send_time[topic] = now

            self._broadcast_to_all_clients(serialized_message, droppable=True)

    def send_unity_service_request(self, topic, service_class, request):
        # 연결된 클라이언트가 없으면 실패
        with self.queues_lock:
            if not self.queues:
                return None

        thread_pauser = ThreadPauser()
        with self.srv_lock:
            srv_id = self.next_srv_id
            self.next_srv_id += 1
            self.services_waiting[srv_id] = thread_pauser

        command = SysCommand_Service()
        command.srv_id = srv_id
        serialized_header = ClientThread.serialize_command("__request", command)
        serialized_message = ClientThread.serialize_message(topic, request)
        self._broadcast_to_all_clients(b"".join([serialized_header, serialized_message]))

        # rospy starts a new thread for each service request,
        # so it won't break anything if we sleep now while waiting for the response
        resumed = thread_pauser.sleep_until_resumed(timeout=30.0)

        if not resumed or thread_pauser.result is None:
            # 타임아웃 또는 결과 없음 - 대기 목록에서 제거
            with self.srv_lock:
                if srv_id in self.services_waiting:
                    del self.services_waiting[srv_id]
            self.tcp_server.logerr(f"Unity service request to '{topic}' timed out")
            return None

        response = deserialize_message(thread_pauser.result, service_class.Response())
        return response

    def send_unity_service_response(self, srv_id, data):
        thread_pauser = None
        with self.srv_lock:
            thread_pauser = self.services_waiting[srv_id]
            del self.services_waiting[srv_id]

        thread_pauser.resume_with_result(data)

    def get_registered_topic(self, topic):
        if topic in self.tcp_server.publishers_table:
            return self.tcp_server.publishers_table[topic]
        elif topic in self.tcp_server.subscribers_table:
            return self.tcp_server.subscribers_table[topic]
        elif topic in self.tcp_server.ros_services_table:
            return self.tcp_server.ros_services_table[topic]
        elif topic in self.tcp_server.unity_services_table:
            return self.tcp_server.unity_services_table[topic]
        else:
            return None

    def send_topic_list(self):
        topic_list = SysCommand_TopicsResponse()
        topics_and_types = self.tcp_server.get_topic_names_and_types()
        topic_list.topics = [item[0] for item in topics_and_types]
        for i in topics_and_types:
            node = self.get_registered_topic(i[0])
            if len(i[1]) > 1:
                if node is not None:
                    self.tcp_server.get_logger().warning(
                        "Only one message type per topic is supported, but found multiple types for topic {}; maintaining {} as the subscribed type.".format(
                            i[0], self.parse_message_name(node.msg)
                        )
                    )
            topic_list.types = [
                item[1][0].replace("/msg/", "/")
                if (len(item[1]) <= 1)
                else self.parse_message_name(node.msg)
                for item in topics_and_types
            ]
        serialized_bytes = ClientThread.serialize_command("__topic_list", topic_list)
        self._broadcast_to_all_clients(serialized_bytes)

    def start_sender(self, conn, halt_event):
        sender_thread = threading.Thread(
            target=self.sender_loop, args=(conn, self.sender_id, halt_event)
        )
        self.sender_id += 1

        # Exit the server thread when the main thread terminates
        sender_thread.daemon = True
        sender_thread.start()

    def sender_loop(self, conn, tid, halt_event):
        # 상한 초과 시 _broadcast_to_all_clients 가 오래된 토픽 메시지를 버린다(0=무제한).
        local_queue = Queue(maxsize=self._queue_maxsize())
        # 이 클라이언트가 마지막으로 보낸 latest_only 메시지 추적
        last_sent = {}  # {topic: serialized_message}

        # send a handshake message to confirm the connection and version number
        handshake_metadata = SysCommand_Handshake_Metadata()
        handshake = SysCommand_Handshake(handshake_metadata)
        local_queue.put(ClientThread.serialize_command("__handshake", handshake))

        # 클라이언트 큐 등록
        with self.queues_lock:
            self.queues[tid] = local_queue

        self.tcp_server.loginfo(f"Client {tid} registered (total: {len(self.queues)} clients)")

        # 클라이언트별 토픽 전송 시간 추적 (스로틀링용)
        last_sent_time = {}  # {topic: timestamp}

        try:
            while not halt_event.is_set():
                now = time.time()

                # 1. latest_only 토픽들의 최신 메시지 전송
                with self.latest_lock:
                    latest_copy = dict(self.latest_messages)

                for topic, msg in latest_copy.items():
                    # 이전에 보낸 메시지와 다를 때만 전송
                    if last_sent.get(topic) is not msg:
                        # 스로틀링 체크
                        policy = self._get_policy(topic)
                        max_freq = policy.get('max_frequency', 0)
                        if max_freq > 0:
                            min_interval = 1.0 / max_freq
                            if now - last_sent_time.get(topic, 0) < min_interval:
                                continue  # 이 토픽은 스킵

                        try:
                            conn.sendall(msg)
                            last_sent[topic] = msg
                            last_sent_time[topic] = now
                        except Exception as e:
                            self.tcp_server.logerr(f"Exception sending latest message: {e}")
                            halt_event.set()
                            break

                if halt_event.is_set():
                    break

                # 2. 큐에서 메시지 가져오기 (짧은 타임아웃)
                try:
                    item = local_queue.get(timeout=0.01)  # 10ms 타임아웃 (더 빠른 반응)
                except Empty:
                    continue

                try:
                    conn.sendall(item)
                except Exception as e:
                    # 소켓이 깨진 상태(피어 리셋/keepalive 만료 등). 스트림 중간에서 실패하면
                    # 프레임 경계를 복구할 수 없으므로 재시도하지 않고 연결을 정리한다
                    # (클라이언트가 재접속하면 새 세션으로 깨끗하게 시작한다).
                    self.tcp_server.logerr("Send failed, closing connection: {}".format(e))
                    break
        finally:
            halt_event.set()
            # 클라이언트 큐 해제
            with self.queues_lock:
                if tid in self.queues:
                    del self.queues[tid]
            self.tcp_server.loginfo(f"Client {tid} unregistered (total: {len(self.queues)} clients)")

    def parse_message_name(self, name):
        try:
            # Example input string: <class 'std_msgs.msg._string.Metaclass_String'>
            names = (str(type(name))).split(".")
            module_name = names[0][8:]
            class_name = names[-1].split("_")[-1][:-2]
            return "{}/{}".format(module_name, class_name)
        except (IndexError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None


class SysCommand_Log:
    def __init__(self):
        self.text = ""


class SysCommand_Service:
    def __init__(self):
        self.srv_id = 0


class SysCommand_TopicsResponse:
    def __init__(self):
        self.topics = []
        self.types = []


class SysCommand_Handshake:
    def __init__(self, metadata):
        self.version = "v0.7.0"
        self.metadata = json.dumps(metadata.__dict__)


class SysCommand_Handshake_Metadata:
    def __init__(self):
        self.protocol = "ROS2"
