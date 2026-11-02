"""실제 두 TCP 연결의 영상 격리·구독 수명·서비스 응답 경쟁을 검증한다."""
import json
import socket
import struct
import threading
import time
from types import SimpleNamespace

import pytest
import rclpy
from std_msgs.msg import Int32

from ros_tcp_endpoint.client import ClientThread
from ros_tcp_endpoint.framed_input import FrameReader
from ros_tcp_endpoint.server import TcpServer


@pytest.fixture
def server():
    rclpy.init()
    instance = TcpServer('wireless_isolation_test')
    yield instance
    instance.destroy_nodes()
    rclpy.shutdown()


def wait_for(predicate):
    deadline = time.monotonic() + 2
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(.001)


class Peer:
    def __init__(self, server):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            self.socket = socket.create_connection(listener.getsockname())
            host, _ = listener.accept()
        self.socket.settimeout(1)
        self.reader = FrameReader(self.socket)
        self.client = ClientThread(host, server, 'local-wireless-test', 0)
        self.client.start()
        assert self.receive().destination == '__handshake'

    def command(self, name, **values):
        raw = json.dumps(values).encode() + b'\0'
        name = ('__' + name).encode()
        self.socket.sendall(struct.pack('<I', len(name)) + name + struct.pack('<I', len(raw)) + raw)

    def receive(self):
        while not self.reader.ready:
            self.reader.feed(self.socket.recv(65536), time.monotonic())
        return self.reader.ready.popleft()

    def close(self):
        self.socket.close()
        self.client.join(3)
        assert not self.client.is_alive()


@pytest.mark.parametrize('policy', ['queue', 'latest_only'])
def test_only_subscribing_peer_receives_image_and_shared_status(server, policy):
    transport = server.unity_tcp_sender
    transport.topic_policies = {'/wireless/image': {'policy': policy}}
    headset, joystick = Peer(server), Peer(server)
    try:
        headset.command('subscribe', topic='/wireless/image', message_name='std_msgs/Int32')
        headset.command('subscribe', topic='/wireless/status', message_name='std_msgs/Int32')
        joystick.command('subscribe', topic='/wireless/status', message_name='std_msgs/Int32')
        wait_for(lambda: transport.has_subscribers('/wireless/image')
                 and sum('/wireless/status' in topics for topics in transport.subscriptions.values()) == 2)
        transport.send_unity_message('/wireless/image', Int32(data=123))
        assert headset.receive().destination == '/wireless/image'
        transport.send_unity_message('/wireless/status', Int32(data=42))
        assert joystick.receive().destination == '/wireless/status'
        assert headset.receive().destination == '/wireless/status'
        joystick.command('remove_subscriber', topic='/wireless/status')
        wait_for(lambda: not transport.wants_topic(joystick.client.conn, '/wireless/status'))
        assert '/wireless/status' in server.subscribers_table
        headset.close()
        wait_for(lambda: '/wireless/image' not in server.subscribers_table
                 and '/wireless/status' not in server.subscribers_table)
        assert not transport.subscriptions.get(headset.client.conn)
    finally:
        if headset.client.is_alive():
            headset.close()
        joystick.close()


def test_one_disconnecting_peer_preserves_other_shared_subscription(server):
    first, second = Peer(server), Peer(server)
    transport = server.unity_tcp_sender
    try:
        for peer in (first, second):
            peer.command('subscribe', topic='/wireless/shared', message_name='std_msgs/Int32')
        wait_for(lambda: sum('/wireless/shared' in topics for topics in transport.subscriptions.values()) == 2)
        node = server.subscribers_table['/wireless/shared']
        first.close()
        assert server.subscribers_table['/wireless/shared'] is node
        transport.send_unity_message('/wireless/shared', Int32(data=8))
        assert second.receive().destination == '/wireless/shared'
    finally:
        if first.client.is_alive():
            first.close()
        second.close()


def test_service_header_response_is_sent_only_to_requesting_peer(server):
    first, second = Peer(server), Peer(server)
    transport = server.unity_tcp_sender
    try:
        transport.send_ros_service_response(7, '/wireless/service', Int32(data=5), conn=first.client.conn)
        assert first.receive().destination == '__response'
        assert first.receive().destination == '/wireless/service'
        transport.send_unity_info('after-service')
        assert second.receive().destination == '__log'
    finally:
        first.close()
        second.close()


def test_syscommand_context_is_per_thread_and_cleared_on_error(server):
    entered, release = threading.Event(), threading.Event()
    first = SimpleNamespace(pending_srv_id=None)
    second = SimpleNamespace(pending_srv_id=None)
    def stalled(srv_id):
        entered.set()
        assert release.wait(2)
        server.syscommands.response(srv_id)
    server.syscommands.stalled = stalled
    thread = threading.Thread(target=server.handle_syscommand, args=('__stalled', b'{"srv_id":11}\0', first))
    thread.start()
    try:
        assert entered.wait(1)
        server.handle_syscommand('__response', b'{"srv_id":22}\0', second)
    finally:
        release.set()
        thread.join(2)
    assert first.pending_srv_id == 11
    assert second.pending_srv_id == 22
    assert server.syscommands.current_client is None
    with pytest.raises(TypeError):
        server.handle_syscommand('__subscribe', b'{"unknown":1}\0', first)
    assert server.syscommands.current_client is None


def test_client_socket_bounds_unsent_video_without_global_network_changes(server):
    option = getattr(socket, 'TCP_NOTSENT_LOWAT', None)
    if option is None:
        pytest.skip('per-socket TCP_NOTSENT_LOWAT is not available')
    with socket.socket() as connection:
        server._configure_client_socket(connection)
        assert connection.getsockopt(socket.IPPROTO_TCP, option) == 16384
