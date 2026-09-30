# ROS TCP Endpoint

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

## Introduction

[ROS](https://www.ros.org/) package used to create an endpoint to accept ROS messages sent from a Unity scene using the [ROS TCP Connector](https://github.com/Unity-Technologies/ROS-TCP-Connector) scripts.

Instructions and examples on how to use this ROS package can be found on the [Unity Robotics Hub](https://github.com/Unity-Technologies/Unity-Robotics-Hub/blob/master/tutorials/ros_unity_integration/README.md) repository.

## Community and Feedback

The Unity Robotics projects are open-source and we encourage and welcome contributions.
If you wish to contribute, be sure to review our [contribution guidelines](CONTRIBUTING.md)
and [code of conduct](CODE_OF_CONDUCT.md).

## Support
For questions or discussions about Unity Robotics package installations or how to best set up and integrate your robotics projects, please create a new thread on the [Unity Robotics forum](https://forum.unity.com/forums/robotics.623/) and make sure to include as much detail as possible.

For feature requests, bugs, or other issues, please file a [GitHub issue](https://github.com/Unity-Technologies/ROS-TCP-Endpoint/issues) using the provided templates and the Robotics team will investigate as soon as possible.

For any other questions or feedback, connect directly with the
Robotics team at [unity-robotics@unity3d.com](mailto:unity-robotics@unity3d.com).

## Teleop input timing

`[M2TeleopStop] pose input timed out` means that the VR control node stopped
teleoperation after 200 ms without a valid pose. It does not mean the process
crashed. A matched ROS publisher or an established TCP connection does not prove
that fresh controller poses are arriving. Restored input requires explicit ON.

The endpoint emits `[TeleopTransport]` at most once per five seconds when a pose
gap, message dispatch or local queue residence exceeds 100 ms, or frames were
coalesced/expired. `wire_read_max_ms` measures time waiting for a complete TCP
frame; `dispatch_max_ms` measures local registration, deserialization and ROS
publishing; `queue_max_ms` measures time since a complete frame was read.
`coalesced` and `expired` count skipped state frames. These are local monotonic durations, not
end-to-end latency or the source timestamp of a pose. A large wire wait points to
the sender/network path; a large dispatch duration points to endpoint handling.
When the peer is completely silent the report appears after traffic resumes.

Identical publisher/subscriber registrations reuse their ROS entities. Repeated
Unity registration previously ran two 50 ms sleeps per entity in the TCP reader,
stalling every input on that connection. Changed message types or publisher
options still replace the entity. TCP QUICKACK is re-armed on Linux after each
received batch to reduce delayed acknowledgements, alongside server NODELAY.
This cannot eliminate headset/Wi-Fi stalls or TCP retransmission delays.

On 2026-09-30, an independent ROS subscriber observed a 365 ms pose gap on the
reported M2 system. Simultaneous ping measured PC-to-AP at mean 3.9 ms / maximum
18.2 ms, and PC-to-headset at mean 32.2 ms / maximum 153.9 ms; other headset
samples reached 737 ms. PC Wi-Fi power saving was already off. After applying
registration reuse and QUICKACK, a 35-second ROS sample received 3,370 poses with a maximum
gap of 177 ms. Longer endpoint observation subsequently caught a 242 ms pose gap
(242 ms wire wait, window maximum local dispatch 2.5 ms). This isolates the remaining delay to the
sender/network path; changing IK or relaxing the stop timeout is not a repair.

### Bounded input and output latency

The TCP reader collects bytes already available without a batching sleep, bounded
to 64 KiB of extra reads, 256 returned frames and a 1 ms drain budget. Within each
batch, repeated endpoint/human poses, hand states and velocity setpoints retain
only their latest value. ON/OFF, recording, system commands and service
header/body pairs remain ordered barriers: state frames never merge across them.
The allowlist is `LATEST_INPUT_TOPICS` in `framed_input.py`.

Allowlisted state frames waiting locally for more than 100 ms are discarded.
Their ROS publishers use history depth 1 with unchanged reliability. These
measures do not determine the original HMD sample age or discard bytes already
queued in the network. No synthetic pose heartbeat is generated, and the M2
200 ms input watchdog is unchanged.

Outgoing updates wake the sender directly instead of waiting for 10 ms polling.
The left/right VR image topics retain one latest image each, at most 30 Hz per
topic, and discard cached images older than 250 ms before sending. Protocol
messages precede the next camera snapshots. A blocked image write already in
progress cannot be preempted. Bounded output queues never evict protocol/service
messages for topic data; if a client fills its queue with control messages alone,
that connection closes instead of blocking the TCP reader or silently corrupting
the protocol. The handshake always precedes cached data on reconnect.

Initial port-bind failure exits the endpoint immediately. It does not leave an
inactive duplicate publishing connection status or serving the same ROS services.
Runtime GUI TCP OFF/ON remains supported.

Regression check (source ROS and the workspace first, use a private domain):

```sh
ROS_DOMAIN_ID=195 python3 -m pytest -q test/test_framed_input.py \
  test/test_input_transport.py test/test_outbound_latency.py \
  test/test_m2_teleop_transport.py
```

All 28 tests passed on 2026-09-30. They cover split/coalesced frames, service/event
ordering, state expiry, ROS delivery, registration reuse, duplicate startup,
output backpressure, wakeups, throttling and reconnects. The optional M2 test
requires the sibling teleoperation source and built workspace; it runs the actual
M2 node in a fresh subprocess/private DDS domain, verifying burst input, a short
gap, sustained loss, process survival and explicit ON after recovery. It skips
when the sibling fixture is absent. The separate M2 invalid/missing pose guard
regression also passed all 5 cases.

A synthetic backlog of 30 poses with 10 ms injected processing cost per pose
delivered the latest in approximately 11 ms and skipped 29 older poses. This is
a server backlog measurement, not a measured wireless latency improvement.
Restart the TCP endpoint to load changes; new live HMD measurements are needed
to assess end-to-end latency, which these arrival-gap diagnostics do not measure.

A subsequent 35-second live run with burst coalescing enabled received 1,861
poses (53.3 Hz after coalescing), with arrival gaps of median 10.0 ms, p95 68.1 ms,
p99 146.4 ms and maximum 176.1 ms. Teleop stayed ON throughout that sample and
reported no new stop. However, the same session had already stopped at 16:03:35
after a 279.6 ms transport gap (278.8 ms read wait, window maximum dispatch
0.4 ms), then resumed after an explicit ON. The changes do not eliminate long
arrival gaps. Packet capture is still needed to separate network arrival from
receiver scheduling; the observed ROS output rate is not the HMD sampling rate.

## License
[Apache License 2.0](LICENSE)
