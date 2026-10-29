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
import re
import uuid

from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSReliabilityPolicy
from rclpy.qos import QoSProfile
from rclpy.serialization import deserialize_message

from .communication import RosReceiver

LATCHED_DISCOVERY_PERIOD_SEC = 0.5
LATCHED_DISCOVERY_RETRIES = 6


class RosSubscriber(RosReceiver):
    """
    Class to send messages outside of ROS network
    """

    def __init__(self, topic, message_class, tcp_server, queue_size=10):
        """

        Args:
            topic:         Topic name to publish messages to
            message_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
        """
        strippedTopic = re.sub("[^A-Za-z0-9_]+", "", topic)
        unique_suffix = uuid.uuid4().hex[:8]
        self.node_name = f"{strippedTopic}_RosSubscriber_{unique_suffix}"
        RosReceiver.__init__(self, self.node_name)
        self.topic = topic
        self.msg = message_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size
        self._is_unregistered = False
        self._latched = self._publishers_all_latched()
        self._last_payload = None
        self._discovery_retries = 0
        self._discovery_timer = None
        self._reregister_timer = None

        # Start Subscriber listener function
        self.subscription = self._create_raw_subscription(self._latched)
        if self._latched:
            self._log("RosSubscriber({}) latched publishers found, TRANSIENT_LOCAL".format(topic))
        else:
            self._discovery_retries = LATCHED_DISCOVERY_RETRIES
            self._discovery_timer = self.create_timer(
                LATCHED_DISCOVERY_PERIOD_SEC, self._check_latched_upgrade)

    @property
    def latched(self):
        return self._latched

    def _create_raw_subscription(self, latched):
        if latched:
            qos_profile = QoSProfile(depth=self.queue_size,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.TRANSIENT_LOCAL
                )
        else:
            qos_profile = QoSProfile(depth=self.queue_size,
                reliability=QoSReliabilityPolicy.BEST_EFFORT,
                durability=QoSDurabilityPolicy.VOLATILE
                )
        return self.create_subscription(
            self.msg, self.topic, self.send_serialized, qos_profile, raw=True
        )

    def _publishers_all_latched(self):
        try:
            infos = self.get_publishers_info_by_topic(self.topic)
        except Exception:
            return False
        return bool(infos) and all(
            info.qos_profile.durability == QoSDurabilityPolicy.TRANSIENT_LOCAL for info in infos
        )

    def _upgrade_to_latched(self):
        if self._is_unregistered or self._latched:
            return
        old = self.subscription
        self._latched = True
        self.subscription = self._create_raw_subscription(True)
        if old is not None:
            self.destroy_subscription(old)
        self._log("RosSubscriber({}) switched to TRANSIENT_LOCAL".format(self.topic))

    def _check_latched_upgrade(self):
        self._discovery_retries -= 1
        if not self._is_unregistered and self._publishers_all_latched():
            self._upgrade_to_latched()
        if self._latched or self._is_unregistered or self._discovery_retries <= 0:
            self._cancel_timer('_discovery_timer')

    def on_reregistered(self):
        if self._is_unregistered or self._reregister_timer is not None:
            return
        self._reregister_timer = self.create_timer(0.01, self._handle_reregistered)
        executor = self.executor
        if executor is not None:
            executor.wake()

    def _handle_reregistered(self):
        self._cancel_timer('_reregister_timer')
        if self._is_unregistered:
            return
        if not self._latched:
            if self._publishers_all_latched():
                self._upgrade_to_latched()
            return
        if self._last_payload is not None and self.count_publishers(self.topic) > 0:
            self.send_serialized(self._last_payload)

    def _cancel_timer(self, attr):
        timer = getattr(self, attr)
        if timer is None:
            return
        setattr(self, attr, None)
        try:
            timer.cancel()
            self.destroy_timer(timer)
        except Exception:
            pass

    def _log(self, text):
        log = getattr(self.tcp_server, 'loginfo', None)
        if log is not None:
            log(text)

    def send_serialized(self, payload):
        """Forward ROS CDR bytes directly; topic type and QoS stay unchanged."""
        if not self._is_unregistered:
            if self._latched:
                self._last_payload = payload
            # Unity's reader expects the local CDR1 little-endian layout.
            # Preserve the previous normalization for other encapsulations.
            if payload[:2] == b'\x00\x01':
                self.tcp_server.send_unity_serialized_message(self.topic, payload)
            else:
                self.tcp_server.send_unity_message(self.topic, deserialize_message(payload, self.msg))
        return self.msg

    def send(self, data):
        """
        Connect to TCP endpoint on client and pass along message
        Args:
            data: message data to send outside of ROS network

        Returns:
            self.msg: The deserialize message

        """
        if self._is_unregistered:
            return self.msg

        self.tcp_server.send_unity_message(self.topic, data)
        return self.msg

    def unregister(self):
        """
        Safely unregister the subscriber.
        destroy_node()는 server.py의 unregister_node()에서 별도로 호출됨.
        """
        self._is_unregistered = True
        self._cancel_timer('_discovery_timer')
        self._cancel_timer('_reregister_timer')

        try:
            if self.subscription is not None:
                self.destroy_subscription(self.subscription)
                self.subscription = None
        except Exception as e:
            if self.tcp_server:
                self.tcp_server.logerr(f"Error destroying subscription for {self.topic}: {e}")
