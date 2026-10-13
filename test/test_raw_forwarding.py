"""Real ROS subscriptions preserve CDR payloads and TCP topic policies."""

import threading
import time
import struct
from types import SimpleNamespace

import pytest
import rclpy
from rclpy.serialization import deserialize_message, serialize_message
from sensor_msgs.msg import Image, LaserScan
from std_msgs.msg import Int32
from tf2_msgs.msg import TFMessage
from geometry_msgs.msg import TransformStamped

from ros_tcp_endpoint.client import ClientThread
from ros_tcp_endpoint.framed_input import FrameReader
from ros_tcp_endpoint.outbound_queue import OutboundQueue
from ros_tcp_endpoint.subscriber import RosSubscriber
from ros_tcp_endpoint.tcp_sender import UnityTcpSender


def messages():
    image = Image(height=48, width=64, encoding='rgb8', step=64 * 3)
    image.header.frame_id = 'camera_optical_frame'
    image.data = bytes(range(256)) * 36
    scan = LaserScan(ranges=[1.0, 2.0, float('inf')], intensities=[3.0, 4.0, 5.0])
    tf = TransformStamped()
    tf.header.frame_id, tf.child_frame_id = 'map', 'base_link'
    tf.transform.translation.x = 1.25
    tf.transform.rotation.w = 1.0
    return [Int32(data=42), image, scan, TFMessage(transforms=[tf])]


@pytest.mark.parametrize('message', messages())
def test_raw_ros_subscription_preserves_cdr_and_message_content(message, monkeypatch):
    rclpy.init()
    transport = UnityTcpSender(SimpleNamespace(loginfo=lambda _: None, logwarn=lambda _: None))
    output = OutboundQueue(10, None, threading.Event())
    transport.queues = {1: output}
    topic = '/raw_forwarding_' + type(message).__name__.lower()
    server = SimpleNamespace(send_unity_serialized_message=transport.send_unity_serialized_message)
    subscriber = RosSubscriber(topic, type(message), server)
    publisher_node = rclpy.create_node('raw_forwarding_publisher')
    publisher = publisher_node.create_publisher(type(message), topic, 10)
    try:
        deadline = time.monotonic() + 2.
        while publisher.get_subscription_count() == 0 and time.monotonic() < deadline:
            rclpy.spin_once(subscriber, timeout_sec=.01)
        assert publisher.get_subscription_count() == 1

        def no_reserialization(*args):
            pytest.fail('Raw subscriber must never deserialize and reserialize ROS data')

        monkeypatch.setattr(ClientThread, 'serialize_message', no_reserialization)
        publisher.publish(message)
        while not output.items and time.monotonic() < deadline:
            rclpy.spin_once(subscriber, timeout_sec=.01)
        assert output.items
        reader = FrameReader(None)
        reader.feed(output.get_nowait(), time.monotonic())
        frame = reader.ready.popleft()
        assert frame.destination == topic
        assert frame.data == serialize_message(message)
        received = deserialize_message(frame.data, type(message))
        assert serialize_message(received) == serialize_message(message)
        assert subscriber.subscription.raw is True
        subscriber.unregister()
        subscriber.send_serialized(serialize_message(message))
        assert not output.items
    finally:
        subscriber.destroy_node()
        publisher_node.destroy_node()
        rclpy.shutdown()


def test_raw_and_typed_framing_are_byte_identical():
    for message in messages():
        topic = '/test/' + type(message).__name__
        assert ClientThread.frame_serialized_message(topic, serialize_message(message)) == \
            ClientThread.serialize_message(topic, message)


def test_raw_latest_snapshot_retains_payload_on_reconnect():
    transport = UnityTcpSender(SimpleNamespace(loginfo=lambda _: None, logwarn=lambda _: None))
    transport.topic_policies = {'/image': {'policy': 'latest_only'}}
    message = messages()[1]
    transport.send_unity_serialized_message('/image', serialize_message(message))
    framed, _ = transport.latest_messages['/image']
    assert framed == ClientThread.serialize_message('/image', message)


def test_big_endian_cdr_is_normalized_for_unity_reader():
    # ROS accepts CDR1 big-endian; Unity's generated reader uses native LE.
    rclpy.init()
    transport = UnityTcpSender(SimpleNamespace(loginfo=lambda _: None, logwarn=lambda _: None))
    output = OutboundQueue(10, None, threading.Event())
    transport.queues = {1: output}
    server = SimpleNamespace(send_unity_serialized_message=transport.send_unity_serialized_message,
                             send_unity_message=transport.send_unity_message)
    subscriber = RosSubscriber('/big_endian', Int32, server)
    try:
        payload = b'\x00\x00\x00\x00' + struct.pack('>i', 42)
        subscriber.send_serialized(payload)
        assert output.get_nowait() == ClientThread.serialize_message('/big_endian', Int32(data=42))
    finally:
        subscriber.destroy_node()
        rclpy.shutdown()
