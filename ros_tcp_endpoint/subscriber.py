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

from rclpy.qos import QoSDurabilityPolicy, QoSHistoryPolicy, QoSReliabilityPolicy
from rclpy.qos import QoSProfile

from .communication import RosReceiver


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
        self.node_name = f"{strippedTopic}_RosSubscriber"
        RosReceiver.__init__(self, self.node_name)
        self.topic = topic
        self.msg = message_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size
        self._is_unregistered = False

        qos_profile = QoSProfile(depth=queue_size,
            reliability=QoSReliabilityPolicy.BEST_EFFORT,
            durability=QoSDurabilityPolicy.VOLATILE
            )

        # Start Subscriber listener function
        self.subscription = self.create_subscription(
            self.msg, self.topic, self.send, qos_profile  # queue_size
        )

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

        try:
            if self.subscription is not None:
                self.destroy_subscription(self.subscription)
                self.subscription = None
        except Exception as e:
            if self.tcp_server:
                self.tcp_server.logerr(f"Error destroying subscription for {self.topic}: {e}")
