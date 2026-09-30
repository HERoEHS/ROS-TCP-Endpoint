"""Regression coverage for live teleop TCP stalls and repeat registration."""

import json
import socket
import struct
import time

import pytest
import rclpy
from rclpy.qos import QoSProfile, ReliabilityPolicy
from tf2_msgs.msg import TFMessage
from geometry_msgs.msg import TransformStamped

from ros_tcp_endpoint.client import ClientThread
from ros_tcp_endpoint.input_timing import InputTiming
from ros_tcp_endpoint.server import TcpServer

TOPIC = '/aeirobot/teleop/endpoint_pose'


@pytest.fixture
def server():
    rclpy.init()
    instance = TcpServer('test_input_transport')
    yield instance
    instance.destroy_nodes()
    rclpy.shutdown()


def test_identical_registration_preserves_entities(server):
    commands = server.syscommands
    commands.publish(TOPIC, 'tf2_msgs/TFMessage')
    commands.subscribe('/aeirobot/teleop/status', 'std_msgs/Int32')
    publisher = server.publishers_table[TOPIC]
    subscriber = server.subscribers_table['/aeirobot/teleop/status']
    start = time.monotonic()
    for _ in range(5):
        commands.publish(TOPIC, 'tf2_msgs/TFMessage')
        commands.subscribe('/aeirobot/teleop/status', 'std_msgs/Int32')
    assert server.publishers_table[TOPIC] is publisher
    assert server.subscribers_table['/aeirobot/teleop/status'] is subscriber
    assert time.monotonic() - start < .2  # previously >= 1 second of fixed sleeps


@pytest.mark.parametrize('change', ['type', 'queue', 'latch'])
def test_changed_registration_replaces_entity(server, change):
    commands = server.syscommands
    commands.publish(TOPIC, 'tf2_msgs/TFMessage')
    old = server.publishers_table[TOPIC]
    commands.publish(TOPIC, 'std_msgs/Bool' if change == 'type' else 'tf2_msgs/TFMessage',
                     queue_size=1 if change == 'queue' else 10, latch=change == 'latch')
    assert server.publishers_table[TOPIC] is not old


def test_timing_distinguishes_wire_wait_from_handler_stall():
    warnings = []
    monitor = InputTiming('test-peer', warnings.append, clock=lambda: 0.)
    monitor.record(TOPIC, 0., 20., 20.001)  # initial connection wait excluded
    monitor.record(TOPIC, 20.001, 20.351, 20.352)
    monitor.record(TOPIC, 25.001, 25.002, 25.003)
    assert 'wire_read_max_ms=350.0' in warnings[0]
    assert 'dispatch_max_ms=1.0' in warnings[0]
    assert '20000' not in warnings[0]
    monitor.record('__publish', 25.003, 25.004, 25.254)
    monitor.record(TOPIC, 25.254, 25.255, 25.256)
    monitor.record(TOPIC, 30.1, 30.101, 30.102)
    assert 'dispatch_max_ms=250.0' in warnings[1]
    assert 'slow_dispatch=__publish' in warnings[1]


@pytest.mark.parametrize('transport', ['unix', 'tcp'])
def test_tcp_repeat_registration_does_not_interrupt_pose_delivery(server, transport):
    """Send real wire frames through ClientThread, observe actual ROS output."""
    server.syscommands.publish(TOPIC, 'tf2_msgs/TFMessage')
    listener = rclpy.create_node('test_pose_listener')
    received = []
    sub = listener.create_subscription(TFMessage, TOPIC,
        lambda msg: received.append((time.monotonic(), msg.transforms[0].header.stamp.sec)),
        QoSProfile(depth=100, reliability=ReliabilityPolicy.BEST_EFFORT))
    deadline = time.monotonic() + 3
    while server.publishers_table[TOPIC].pub.get_subscription_count() == 0:
        assert time.monotonic() < deadline
        rclpy.spin_once(listener, timeout_sec=.01)
    if transport == 'tcp':
        with socket.socket() as acceptor:
            acceptor.bind(('127.0.0.1', 0))
            acceptor.listen(1)
            peer = socket.create_connection(acceptor.getsockname())
            host, _ = acceptor.accept()
    else:
        host, peer = socket.socketpair()
    client = ClientThread(host, server, 'local-test', 0)
    client.start()
    try:
        raw = json.dumps({'topic': TOPIC, 'message_name': 'tf2_msgs/TFMessage'}).encode() + b'\0'
        dest = b'__publish'
        registration = struct.pack('<I', len(dest)) + dest + struct.pack('<I', len(raw)) + raw
        for i in range(8):
            tf = TransformStamped()
            tf.header.stamp.sec = i
            peer.sendall(registration + ClientThread.serialize_message(TOPIC, TFMessage(transforms=[tf])))
            end = time.monotonic() + .015
            while time.monotonic() < end:
                rclpy.spin_once(listener, timeout_sec=.001)
        deadline = time.monotonic() + .15
        while len(received) < 8 and time.monotonic() < deadline:
            rclpy.spin_once(listener, timeout_sec=.005)
        assert [value for _, value in received] == list(range(8))
        assert max(b[0]-a[0] for a, b in zip(received, received[1:])) < .2
        # A connected but silent peer must not manufacture pose heartbeats.
        end = time.monotonic() + .25
        while time.monotonic() < end:
            rclpy.spin_once(listener, timeout_sec=.01)
        assert len(received) == 8
    finally:
        peer.close()
        client.join(timeout=2)
        assert not client.is_alive()
        listener.destroy_node()


def test_healthy_stream_has_no_gap_warning():
    warnings = []
    monitor = InputTiming('test-peer', warnings.append, clock=lambda: 0.)
    for i in range(1000):
        t = i * .01
        monitor.record(TOPIC, t, t + .0001, t + .0002)
    assert not warnings


def test_delayed_burst_delivers_newest_pose_without_replaying_old_frames(server, record_property):
    """A 300 ms publish backlog must collapse before ROS processing starts."""
    server.syscommands.publish(TOPIC, 'tf2_msgs/TFMessage')
    publisher = server.publishers_table[TOPIC]
    delivered = []
    from rclpy.serialization import deserialize_message
    def slow_publish(data):
        time.sleep(.01)  # emulate a locally backpressured ROS writer
        delivered.append((time.monotonic(), deserialize_message(data, TFMessage).transforms[0].header.stamp.sec))
    publisher.send = slow_publish
    frames = []
    for i in range(30):
        tf = TransformStamped()
        tf.header.stamp.sec = i
        frames.append(ClientThread.serialize_message(TOPIC, TFMessage(transforms=[tf])))
    host, peer = socket.socketpair()
    client = ClientThread(host, server, 'burst-test', 0)
    try:
        peer.sendall(b''.join(frames))
        start = time.monotonic()
        client.start()
        deadline = start + .15
        while not delivered and time.monotonic() < deadline:
            time.sleep(.001)
        assert [value for _, value in delivered] == [29]
        elapsed_ms = (delivered[0][0] - start) * 1000
        record_property('latest_pose_delivery_ms', elapsed_ms)
        record_property('coalesced_frames', client.input_timing.coalesced)
        assert elapsed_ms < 150.
        assert client.input_timing.coalesced == 29
        assert publisher.pub.qos_profile.depth == 1
        assert client.reader.read_calls < 10  # formerly 4 reads per frame
    finally:
        peer.close()
        client.join(timeout=2)
        assert not client.is_alive()


def test_local_handler_stall_expires_queued_pose_without_renewing_it(server):
    server.syscommands.publish(TOPIC, 'tf2_msgs/TFMessage')
    delivered = []
    server.publishers_table[TOPIC].send = delivered.append
    original = server.handle_syscommand
    def stalled_command(destination, data, client):
        if destination == '__test_stall':
            time.sleep(.15)
        else:
            original(destination, data, client)
    server.handle_syscommand = stalled_command
    host, peer = socket.socketpair()
    client = ClientThread(host, server, 'stall-test', 0)
    try:
        destination = b'__test_stall'
        stall = struct.pack('<I', len(destination)) + destination + struct.pack('<I', 0)
        pose = ClientThread.serialize_message(TOPIC, TFMessage())
        peer.sendall(stall + pose)
        client.start()
        deadline = time.monotonic() + 1
        while client.input_timing.expired == 0 and time.monotonic() < deadline:
            time.sleep(.001)
        assert client.input_timing.expired == 1
        assert not delivered
        peer.sendall(pose)
        deadline = time.monotonic() + .1
        while not delivered and time.monotonic() < deadline:
            time.sleep(.001)
        assert len(delivered) == 1  # only the newly received packet was published
    finally:
        peer.close()
        client.join(timeout=2)
        assert not client.is_alive()


def test_duplicate_startup_fails_without_publishing_false_status(server):
    with socket.socket() as owner:
        owner.bind(('127.0.0.1', 0))
        owner.listen(1)
        server.tcp_ip, server.tcp_port = owner.getsockname()
        published_status = []
        server._publish_enabled = published_status.append
        with pytest.raises(OSError):
            server.start()
        server.stop()
        assert not server._started
        assert server._server_thread is None
        assert published_status == []


def test_successful_start_and_runtime_tcp_toggle_still_work(server):
    server.tcp_ip, server.tcp_port = '127.0.0.1', 0
    server.start()
    assert server.tcp_enabled
    port = server.server_socket.getsockname()[1]
    assert port > 0
    assert server.set_tcp_enabled(False)[0]
    assert not server.tcp_enabled
    assert server.set_tcp_enabled(True)[0]
    assert server.tcp_enabled
