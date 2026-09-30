#!/usr/bin/env python

import rclpy

from ros_tcp_endpoint import TcpServer


def main(args=None):
    rclpy.init(args=args)
    tcp_server = TcpServer("UnityEndpoint")
    try:
        tcp_server.start()
        tcp_server.setup_executor()
    except KeyboardInterrupt:
        pass
    except OSError as exc:
        tcp_server.logerr('TCP endpoint startup failed: {}. Stop the duplicate endpoint or use another port.'.format(exc))
        raise SystemExit(1)
    finally:
        tcp_server.destroy_nodes()
        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
