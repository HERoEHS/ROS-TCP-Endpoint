"""Slow clients, control frames and latest-image scheduling regressions."""

import socket
import threading
import time
from types import SimpleNamespace

from rclpy.serialization import deserialize_message
from std_msgs.msg import Int32

from ros_tcp_endpoint.framed_input import FrameReader
from ros_tcp_endpoint.outbound_queue import OutboundQueue
from ros_tcp_endpoint.tcp_sender import UnityTcpSender


def sender():
    warnings = []
    server = SimpleNamespace(logwarn=warnings.append, loginfo=lambda text: None,
                             logerr=warnings.append)
    return UnityTcpSender(server), warnings


def wait_for(predicate, timeout=1.):
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline
        time.sleep(.001)


def test_data_overflow_preserves_control_frames_and_order():
    queue = OutboundQueue(3, None, threading.Event())
    queue.offer(b'control1')
    queue.offer(b'old_image', droppable=True)
    queue.offer(b'control2')
    assert queue.offer(b'latest_image', droppable=True) == (True, 1)
    assert [queue.get_nowait() for _ in range(3)] == [b'control1', b'control2', b'latest_image']
    for value in (b'a', b'b', b'c'):
        queue.offer(value)
    assert queue.offer(b'data', droppable=True) == (False, 1)
    assert [queue.get_nowait() for _ in range(3)] == [b'a', b'b', b'c']


def test_control_overflow_disconnects_only_slow_client_without_blocking_input():
    transport, warnings = sender()
    slow, peer = socket.socketpair()
    halt = threading.Event()
    saturated = OutboundQueue(1, slow, halt)
    saturated.offer(b'first_control')
    healthy = OutboundQueue(2, None, threading.Event())
    transport.queues = {0: saturated, 1: healthy}
    try:
        start = time.monotonic()
        transport._broadcast_to_all_clients(b'next_control')
        assert time.monotonic() - start < .1  # previously a one-second blocking put
        assert halt.is_set()
        peer.settimeout(.1)
        assert peer.recv(1) == b''
        assert healthy.get_nowait() == b'next_control'
        assert saturated.get_nowait() == b'first_control'
        assert warnings
    finally:
        slow.close()
        peer.close()


class ControlledConnection:
    """Pause the first data write as if the HMD stopped reading its socket."""

    def __init__(self, block_first_data=False):
        self.sent = []
        self.block_first_data = block_first_data
        self.blocked = threading.Event()
        self.release = threading.Event()

    def sendall(self, data):
        reader = FrameReader(None)
        reader.feed(data, time.monotonic())
        frame = reader.ready.popleft()
        if self.block_first_data and len(self.sent) == 1:
            self.blocked.set()
            assert self.release.wait(2.)
        self.sent.append(frame)


def start(transport, conn):
    halt = threading.Event()
    thread = threading.Thread(target=transport.sender_loop, args=(conn, 1, halt))
    thread.start()
    wait_for(lambda: bool(conn.sent))
    return halt, thread


def stop(transport, halt, thread):
    halt.set()
    for queue in list(transport.queues.values()):
        queue.wake.set()
    thread.join(timeout=2)
    assert not thread.is_alive()


def test_slow_image_link_keeps_latest_and_control_precedes_image_backlog():
    transport, _ = sender()
    transport.topic_policies = {'/image': {'policy': 'latest_only'}}
    transport.send_unity_message('/image', Int32(data=0))
    conn = ControlledConnection(block_first_data=True)
    halt, thread = start(transport, conn)
    try:
        assert conn.sent[0].destination == '__handshake'
        assert conn.blocked.wait(1.)
        for i in range(1, 101):
            transport.send_unity_message('/image', Int32(data=i))
        transport.send_unity_info('control during image backlog')
        conn.release.set()
        wait_for(lambda: len(conn.sent) >= 4)
        assert [frame.destination for frame in conn.sent] == ['__handshake', '/image', '__log', '/image']
        assert deserialize_message(conn.sent[-1].data, Int32).data == 100
    finally:
        conn.release.set()
        stop(transport, halt, thread)


def test_latest_message_wakes_sender_and_is_not_repeated_as_keepalive(record_property):
    transport, _ = sender()
    transport.topic_policies = {'/state': {'policy': 'latest_only'}}
    conn = ControlledConnection()
    halt, thread = start(transport, conn)
    try:
        time.sleep(.003)  # sender is waiting on work rather than a polling loop
        start_time = time.monotonic()
        transport.send_unity_message('/state', Int32(data=5))
        wait_for(lambda: len(conn.sent) == 2, timeout=.1)
        record_property('latest_send_latency_ms', (conn.sent[-1].received_at - start_time) * 1000)
        time.sleep(.025)
        assert len(conn.sent) == 2
    finally:
        stop(transport, halt, thread)


def test_expired_cached_image_is_not_replayed_to_reconnecting_hmd():
    transport, _ = sender()
    transport.topic_policies = {'/image': {'policy': 'latest_only', 'max_age': .01}}
    transport.send_unity_message('/image', Int32(data=1))
    time.sleep(.02)
    conn = ControlledConnection()
    halt, thread = start(transport, conn)
    try:
        time.sleep(.02)
        assert [frame.destination for frame in conn.sent] == ['__handshake']
        transport.send_unity_message('/image', Int32(data=2))
        wait_for(lambda: len(conn.sent) == 2)
        assert deserialize_message(conn.sent[-1].data, Int32).data == 2
    finally:
        stop(transport, halt, thread)


def test_throttled_latest_message_is_sent_when_due_without_another_notification():
    transport, _ = sender()
    transport.topic_policies = {'/state': {'policy': 'latest_only', 'max_frequency': 20}}
    conn = ControlledConnection()
    halt, thread = start(transport, conn)
    try:
        transport.send_unity_message('/state', Int32(data=0))
        wait_for(lambda: len(conn.sent) == 2)
        transport.send_unity_message('/state', Int32(data=1))
        transport.send_unity_message('/state', Int32(data=2))
        wait_for(lambda: len(conn.sent) == 3)
        assert deserialize_message(conn.sent[-1].data, Int32).data == 2
        assert conn.sent[-1].received_at - conn.sent[-2].received_at >= .048
    finally:
        stop(transport, halt, thread)
