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

        self.unity_tcp_sender = UnityTcpSender(self)

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

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            tcp_server.bind((self.tcp_ip, self.tcp_port))
        except OSError as e:
            self.logerr("Failed to bind to {}:{} - {}".format(self.tcp_ip, self.tcp_port, e))
            self.logerr("Port may already be in use. Please check and try again.")
            return

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                # 소켓 최적화: 지연 최소화 및 연결 감지
                conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                ClientThread(conn, self, ip, port).start()
            except socket.timeout as err:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

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

    def destroy_nodes(self):
        """
            Clean up all of the nodes
        """
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
