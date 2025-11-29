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
import re
import time
import uuid

from rclpy.serialization import deserialize_message

from .communication import RosSender


class RosService(RosSender):
    """
    Class to send messages to a ROS service.
    """

    def __init__(self, service, service_class):
        """
        Args:
            service:        The service name in ROS
            service_class:  The service class in catkin workspace
        """
        strippedService = re.sub("[^A-Za-z0-9_]+", "", service)
        unique_suffix = uuid.uuid4().hex[:8]
        node_name = f"{strippedService}_RosService_{unique_suffix}"
        RosSender.__init__(self, node_name)

        self.service_topic = service
        self.cli = self.create_client(service_class, service)
        self.req = service_class.Request()

    def send(self, data):
        """
        Takes in serialized message data from source outside of the ROS network,
        deserializes it into it's class, calls the service with the message, and returns
        the service's response.

        Args:
            data: The already serialized message_class data coming from outside of ROS

        Returns:
            service response
        """
        message_type = type(self.req)
        message = deserialize_message(data, message_type)

        if not self.cli.service_is_ready():
            self.get_logger().error(
                "Ignoring service call to {} - service is not ready.".format(self.service_topic)
            )
            return None

        future = self.cli.call_async(message)

        # spin_until_future_complete 대신 타임아웃과 함께 대기
        timeout_sec = 30.0
        start_time = self.get_clock().now()

        while rclpy.ok():
            if future.done():
                try:
                    return future.result()
                except Exception as e:
                    self.get_logger().error(f"Service call failed: {e}")
                    return None

            # 타임아웃 체크
            elapsed = (self.get_clock().now() - start_time).nanoseconds / 1e9
            if elapsed > timeout_sec:
                self.get_logger().error(
                    f"Service call to {self.service_topic} timed out after {timeout_sec}s"
                )
                return None

            # CPU 사용량 감소를 위한 짧은 sleep
            time.sleep(0.01)

        return None

    def unregister(self):
        """

        Returns:

        """
        self.destroy_client(self.cli)
        self.destroy_node()
