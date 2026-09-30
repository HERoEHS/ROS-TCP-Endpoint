"""Optional workspace integration: actual M2 node driven through TCP framing."""

from pathlib import Path
import os
import socket
import subprocess
import sys

import pytest


def test_tcp_bursts_keep_m2_active_but_loss_requires_explicit_restart(tmp_path, monkeypatch):
    vr = Path(__file__).resolve().parents[2] / 'aeirobot_teleoperation/aeirobot_vr'
    if not (vr / 'test/m2_feedback_guard_test.py').exists():
        pytest.skip('Requires the M2 teleoperation workspace integration fixture')
    if os.environ.get('M2_TCP_TEST_CHILD') != '1':
        # DDS middleware/configuration may already be cached by preceding ROS
        # tests. Initialize this private-domain integration in a fresh process.
        result = subprocess.run(
            [sys.executable, '-m', 'pytest', '-q', str(Path(__file__).resolve())],
            env=dict(os.environ, M2_TCP_TEST_CHILD='1'), capture_output=True,
            text=True, timeout=30.)
        assert result.returncode == 0, result.stdout + result.stderr
        return
    monkeypatch.syspath_prepend(str(vr / 'test'))
    from ament_index_python.packages import get_package_prefix
    from m2_feedback_guard_test import FeedbackHarness
    import rclpy
    from ros_tcp_endpoint.client import ClientThread
    from ros_tcp_endpoint.server import TcpServer

    monkeypatch.setenv('VR_TEST_CONFIG_DIR', str(vr / 'config'))
    monkeypatch.setenv('VR_CONTROL_NODE', str(Path(get_package_prefix('aeirobot_vr')) /
                                            'lib/aeirobot_vr/aeirobot_vr_control_node'))
    fixture = FeedbackHarness(tmp_path, monkeypatch, joint=None)
    # FeedbackHarness selects a private localhost-only DDS domain before this
    # default context is initialized. No user-domain robot command is sent.
    rclpy.init()
    server = TcpServer('m2_tcp_input_regression')
    pose_topic = '/aeirobot/teleop/endpoint_pose'
    onoff_topic = '/aeirobot/teleop/onoff'
    server.syscommands.publish(pose_topic, 'tf2_msgs/TFMessage')
    server.syscommands.publish(onoff_topic, 'std_msgs/Bool')
    host, peer = socket.socketpair()
    client = ClientThread(host, server, 'm2-private-test', 0)

    class WirePublisher:
        def __init__(self, topic, batch=1):
            self.topic = topic
            self.batch = batch
            self.calls = 0

        def publish(self, msg):
            self.calls += 1
            if self.calls % self.batch == 0:
                peer.sendall(ClientThread.serialize_message(self.topic, msg) * self.batch)

        def get_subscription_count(self):
            return server.publishers_table[self.topic].pub.get_subscription_count()

    fixture.node.destroy_publisher(fixture.pose)
    fixture.node.destroy_publisher(fixture.onoff)
    fixture.pose = WirePublisher(pose_topic, batch=5)
    fixture.onoff = WirePublisher(onoff_topic)
    client.start()
    try:
        fixture.activate()
        fixture.hold(.7, require_active=True)
        assert client.input_timing.coalesced > 0
        fixture.pose_enabled = False
        fixture.hold(.07, require_active=True)
        fixture.pose_enabled = True
        fixture.hold(.2, require_active=True)
        fixture.pose_enabled = False
        fixture.wait(lambda: fixture.status == 0, timeout=1.)
        fixture.wait(lambda: bool(fixture.error_messages), timeout=1.)
        assert 'pose input timed out' in fixture.error_messages[-1]
        assert fixture.process.poll() is None
        fixture.pose_enabled = True
        fixture.wait(lambda: fixture.status == 1)
        before = len(fixture.commands)
        fixture.hold(.3)
        assert len(fixture.commands) == before, 'TCP recovery resumed teleop without ON'
        fixture.activate()
        fixture.hold(.2, require_active=True)
    finally:
        fixture.pose_enabled = False
        peer.close()
        client.join(timeout=2)
        assert not client.is_alive()
        fixture.close()
        server.destroy_nodes()
        rclpy.shutdown()
