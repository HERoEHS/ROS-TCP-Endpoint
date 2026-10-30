"""TRANSIENT_LOCAL publishers reach Unity with their last value, also after re-registration."""

import time
from types import SimpleNamespace

import rclpy
from rclpy.qos import QoSDurabilityPolicy, QoSProfile, QoSReliabilityPolicy
from rclpy.serialization import deserialize_message
from std_msgs.msg import Bool

from ros_tcp_endpoint.subscriber import RosSubscriber

LATCHED = QoSProfile(depth=1, reliability=QoSReliabilityPolicy.RELIABLE,
                     durability=QoSDurabilityPolicy.TRANSIENT_LOCAL)
VOLATILE = QoSProfile(depth=10)


def spin_until(nodes, condition, timeout=3.):
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        for node in nodes:
            rclpy.spin_once(node, timeout_sec=.01)
    return condition()


def make_server(received):
    return SimpleNamespace(
        send_unity_serialized_message=lambda topic, data: received.append(
            deserialize_message(data, Bool).data),
        loginfo=lambda _: None)


def test_latched_publisher_last_value_reaches_late_subscriber():
    rclpy.init()
    publisher_node = rclpy.create_node('latched_publisher')
    publisher = publisher_node.create_publisher(Bool, '/latched_state', LATCHED)
    publisher.publish(Bool(data=True))
    received = []
    subscriber = None
    try:
        spin_until([publisher_node], lambda: False, timeout=.5)
        subscriber = RosSubscriber('/latched_state', Bool, make_server(received))
        assert spin_until([subscriber, publisher_node], lambda: subscriber.latched)
        assert spin_until([subscriber, publisher_node], lambda: received == [True])

        subscriber.on_reregistered()
        assert spin_until([subscriber, publisher_node], lambda: received == [True, True])
    finally:
        if subscriber is not None:
            subscriber.unregister()
            subscriber.destroy_node()
        publisher_node.destroy_node()
        rclpy.shutdown()


def test_volatile_publisher_keeps_best_effort_volatile_subscription():
    rclpy.init()
    publisher_node = rclpy.create_node('volatile_publisher')
    publisher_node.create_publisher(Bool, '/volatile_state', VOLATILE)
    subscriber = None
    try:
        spin_until([publisher_node], lambda: False, timeout=.5)
        subscriber = RosSubscriber('/volatile_state', Bool, make_server([]))
        spin_until([subscriber, publisher_node], lambda: False, timeout=1.)
        assert not subscriber.latched
    finally:
        if subscriber is not None:
            subscriber.unregister()
            subscriber.destroy_node()
        publisher_node.destroy_node()
        rclpy.shutdown()


def test_subscription_upgrades_when_latched_publisher_appears_later():
    rclpy.init()
    received = []
    subscriber = RosSubscriber('/late_latched_state', Bool, make_server(received))
    publisher_node = None
    try:
        assert not subscriber.latched
        publisher_node = rclpy.create_node('late_latched_publisher')
        publisher = publisher_node.create_publisher(Bool, '/late_latched_state', LATCHED)
        publisher.publish(Bool(data=False))
        assert spin_until([subscriber, publisher_node], lambda: subscriber.latched, timeout=4.)
        assert spin_until([subscriber, publisher_node], lambda: False in received)
    finally:
        subscriber.unregister()
        subscriber.destroy_node()
        if publisher_node is not None:
            publisher_node.destroy_node()
        rclpy.shutdown()
