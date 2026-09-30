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
import struct
import socket
import time

import threading
import json

from rclpy.serialization import deserialize_message
from rclpy.serialization import serialize_message

from .exceptions import TopicOrServiceNameDoesNotExistError
from .input_timing import InputTiming
from .framed_input import FrameReader, LATEST_INPUT_TOPICS, latest_frames


class ClientThread(threading.Thread):
    """
    Thread class to read all data from a connection and pass along the data to the
    desired source.
    """

    def __init__(self, conn, tcp_server, incoming_ip, incoming_port):
        """
        Set class variables
        Args:
            conn:
            tcp_server: server object
            incoming_ip: connected from this IP address
            incoming_port: connected from this port
        """
        self.conn = conn
        self.tcp_server = tcp_server
        self.incoming_ip = incoming_ip
        self.incoming_port = incoming_port
        # 클라이언트별 서비스 요청 상태 (race condition 방지)
        self.pending_srv_id = None
        self.pending_srv_is_request = False
        self.input_timing = InputTiming(incoming_ip, tcp_server.logwarn)
        self.quickack = getattr(socket, 'TCP_QUICKACK', None)
        self.reader = FrameReader(conn)
        threading.Thread.__init__(self)

    @staticmethod
    def recvall(conn, size, flags=0):
        """
        Receive exactly bufsize bytes from the socket.
        """
        buffer = bytearray(size)
        view = memoryview(buffer)
        pos = 0
        while pos < size:
            read = conn.recv_into(view[pos:], size - pos, flags)
            if not read:
                raise IOError("No more data available")
            pos += read
        return bytes(buffer)

    @staticmethod
    def read_int32(conn):
        """
        Reads four bytes from socket connection and unpacks them to an int

        Returns: int

        """
        raw_bytes = ClientThread.recvall(conn, 4)
        num = struct.unpack("<I", raw_bytes)[0]
        return num

    def read_string(self):
        """
        Reads int32 from socket connection to determine how many bytes to
        read to get the string that follows. Read that number of bytes and
        decode to utf-8 string.

        Returns: string

        """
        str_len = ClientThread.read_int32(self.conn)

        str_bytes = ClientThread.recvall(self.conn, str_len)
        decoded_str = str_bytes.decode("utf-8")

        return decoded_str

    # 최대 메시지 크기 (100MB) - 메모리 공격 방지
    MAX_MESSAGE_SIZE = 100 * 1024 * 1024

    def read_message(self, conn):
        """
        Decode destination and full message size from socket connection.
        Grab bytes in chunks until full message has been read.
        """
        data = b""

        destination = self.read_string()
        full_message_size = ClientThread.read_int32(conn)

        # 메시지 크기 검증
        if full_message_size > self.MAX_MESSAGE_SIZE:
            self.tcp_server.logerr(
                "Message too large: {} bytes (max: {} bytes)".format(
                    full_message_size, self.MAX_MESSAGE_SIZE
                )
            )
            return None

        data = ClientThread.recvall(conn, full_message_size)

        if full_message_size > 0 and not data:
            self.tcp_server.logerr("No data for a message size of {}, breaking!".format(full_message_size))
            return None

        destination = destination.rstrip("\x00")
        return destination, data

    @staticmethod
    def serialize_message(destination, message):
        """
        Serialize a destination and message class.

        Args:
            destination: name of destination
            message:     message class to serialize

        Returns:
            serialized destination and message as a list of bytes
        """
        dest_bytes = destination.encode("utf-8")
        length = len(dest_bytes)
        dest_info = struct.pack("<I%ss" % length, length, dest_bytes)

        serial_response = serialize_message(message)

        msg_length = struct.pack("<I", len(serial_response))
        serialized_message = dest_info + msg_length + serial_response

        return serialized_message

    @staticmethod
    def serialize_command(command, params):
        cmd_bytes = command.encode("utf-8")
        cmd_length = len(cmd_bytes)
        cmd_info = struct.pack("<I%ss" % cmd_length, cmd_length, cmd_bytes)

        json_bytes = json.dumps(params.__dict__).encode("utf-8")
        json_length = len(json_bytes)
        json_info = struct.pack("<I%ss" % json_length, json_length, json_bytes)

        return cmd_info + json_info

    def send_ros_service_request(self, srv_id, destination, data):
        if destination not in self.tcp_server.ros_services_table.keys():
            error_msg = "Service destination '{}' is not registered! Known services are: {} ".format(
                destination, self.tcp_server.ros_services_table.keys()
            )
            self.tcp_server.send_unity_error(error_msg)
            self.tcp_server.logerr(error_msg)
            # TODO: send a response to Unity anyway?
            return
        else:
            ros_communicator = self.tcp_server.ros_services_table[destination]
            service_thread = threading.Thread(
                target=self.service_call_thread, args=(srv_id, destination, data, ros_communicator)
            )
            service_thread.daemon = True
            service_thread.start()

    def service_call_thread(self, srv_id, destination, data, ros_communicator):
        response = ros_communicator.send(data)

        if not response:
            error_msg = "No response data from service '{}'!".format(destination)
            self.tcp_server.send_unity_error(error_msg)
            self.tcp_server.logerr(error_msg)
            # TODO: send a response to Unity anyway?
            return

        self.tcp_server.unity_tcp_sender.send_ros_service_response(srv_id, destination, response)

    def dispatch(self, destination, data):
        """Dispatch one ordered frame, preserving service header/body pairing."""
        if self.pending_srv_id is not None:
            if self.pending_srv_is_request:
                self.send_ros_service_request(self.pending_srv_id, destination, data)
            else:
                self.tcp_server.send_unity_service_response(self.pending_srv_id, data)
            self.pending_srv_id = None
        elif destination == '':
            pass
        elif destination.startswith('__'):
            self.tcp_server.handle_syscommand(destination, data, self)
        elif destination in self.tcp_server.publishers_table:
            self.tcp_server.publishers_table[destination].send(data)
        else:
            error_msg = "Not registered to publish topic '{}'! Valid publish topics are: {} ".format(
                destination, self.tcp_server.publishers_table.keys())
            self.tcp_server.send_unity_error(error_msg)
            self.tcp_server.logerr(error_msg)

    def run(self):
        """
        Receive a message from Unity and determine where to send it based on the publishers table
         and topic string. Then send the read message.

        If there is a response after sending the serialized data, assume it is a
        ROS service response.

        Message format is expected to arrive as
            int: length of destination bytes
            str: destination. Publisher topic, Subscriber topic, Service name, etc
            int: size of full message
            msg: the ROS msg type as bytes

        """
        self.tcp_server.loginfo("Connection from {}".format(self.incoming_ip))
        halt_event = threading.Event()
        sender_thread = self.tcp_server.unity_tcp_sender.start_sender(self.conn, halt_event)
        try:
            while not halt_event.is_set():
                read_started = time.monotonic()
                batch = self.reader.read_batch()
                # NODELAY governs server writes only. Promptly ACK incoming small
                # controller frames too; Linux QUICKACK is transient, so re-arm.
                if self.quickack is not None:
                    try:
                        self.conn.setsockopt(socket.IPPROTO_TCP, self.quickack, 1)
                    except OSError:
                        self.quickack = None  # unsupported socket/platform; continue

                selected = latest_frames(batch, self.pending_srv_id is not None)
                self.input_timing.coalesced += len(batch) - len(selected)
                for frame in selected:
                    if halt_event.is_set() or self.conn.fileno() < 0:
                        break
                    processing_started = time.monotonic()
                    # Only local residence time is known; do not pretend this is
                    # the HMD sample age. Never replay a queued pose as a heartbeat.
                    if (self.pending_srv_id is None and frame.destination in LATEST_INPUT_TOPICS
                            and processing_started - frame.received_at > .1):
                        self.input_timing.expired += 1
                    else:
                        self.dispatch(frame.destination, frame.data)
                    self.input_timing.record(frame.destination, read_started, frame.received_at,
                                             time.monotonic(), processing_started)
                    read_started = frame.received_at
        except (IOError, OSError) as e:
            # OFF 전환/종료로 소켓이 닫혀 발생하는 예외는 로그를 억제한다(tcp_enabled 일 때만 로깅).
            if self.tcp_server.tcp_enabled:
                self.tcp_server.logerr("Exception: {}".format(e))
        except (struct.error, UnicodeDecodeError, ValueError) as e:
            # 손상/절단된 프레임. 길이 헤더를 신뢰할 수 없어 스트림 동기를 되찾을 수 없으므로
            # 연결을 끊고 클라이언트의 재접속을 기다린다. (종전에는 스레드가 조용히 죽어
            # 원인 없이 "연결이 끊겼다"로만 보였다.)
            self.tcp_server.logerr("Malformed frame from {} — closing connection: {}".format(
                self.incoming_ip, e))
        except Exception as e:
            # 메시지 처리 중 예기치 못한 예외로 수신 스레드가 통째로 죽는 것을 막는다.
            self.tcp_server.logerr("Unexpected error handling client {}: {}".format(
                self.incoming_ip, e))
        finally:
            halt_event.set()
            try:
                self.conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                self.conn.close()
            except OSError:
                pass
            sender_thread.join(timeout=1.0)
            # 상태 목록(_client_sockets)에서 이 연결 제거 → gui status 반영.
            self.tcp_server.unregister_client(self.conn)
            self.tcp_server.loginfo("Disconnected from {}".format(self.incoming_ip))
