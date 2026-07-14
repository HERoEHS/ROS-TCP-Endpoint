from launch import LaunchDescription
from launch_ros.actions import Node
from ament_index_python.packages import get_package_share_directory
import os


def generate_launch_description():
    # alice_parameters install share의 active 디렉토리에서 로드 (alice_activate.sh로 robot별 활성화)
    param_path = os.path.join(
        get_package_share_directory("alice_parameters"),
        "config", "active", "ros_tcp_endpoint_param.yaml",
    )
    return LaunchDescription(
        [
            Node(
                package="ros_tcp_endpoint",
                executable="default_server_endpoint",
                emulate_tty=True,
                parameters=[param_path],
            )
        ]
    )
