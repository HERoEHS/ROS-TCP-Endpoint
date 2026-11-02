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
batch, short groups of up to four samples per state topic retain every real
sample. Larger state backlogs retain only their latest value. ON/OFF, recording, system commands and service
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

## M2 100Hz 입력과 무선 단말 격리 (2026-11-02)

TCP는 100Hz 입력도 2~4개씩 묶어서 전달할 수 있습니다. 기존 수신부가 짧은 묶음도 한 개로
줄이던 동작을 수정했습니다. 큰 밀림·로컬 100ms 만료 입력은 계속 정리하며, 마지막 값을
반복하거나 보간해서 Hz를 올리지 않습니다. M2의 200ms 입력 워치독과 복구 후 명시적 ON 규칙은 유지합니다.

- 각 Unity 연결에는 그 단말이 구독한 토픽만 보냅니다. HMD 영상을 구독하지 않은 조이스틱으로
  영상을 복제하지 않습니다. 공동 구독은 마지막 단말이 해제할 때까지 유지합니다.
- ROS 서비스 응답과 토픽 목록은 요청한 연결로만 보내고, 서비스 헤더 상태를 스레드별로 분리합니다.
  연결 끊김은 ROS 구독 정리보다 먼저 알립니다.
- 명시한 토픽 정책이 없으면 Image·CompressedImage는 최신 한 장·최대30Hz·대기 수명250ms로
  처리합니다. 명시적 토픽 설정이 우선하며 이벤트·서비스를 영상처럼 합치지 않습니다.
- 서비스·프로토콜 응답은 토픽 대기열과 좌/우 영상 사이에서 우선합니다. Linux는 연결별
  `TCP_NOTSENT_LOWAT=16384`로 아직 보내지 않은 영상의 커널 적체를 제한합니다(0이면 사용 안 함).
  호스트의 전역 네트워크 설정은 바꾸지 않습니다. [Linux 커널 설명](https://kernel.org/doc/html/latest/networking/ip-sysctl.html#tcp-notsent-lowat-unsigned-integer)을 참고하세요.
- 활성 pose 스트림은 5초마다 `[TeleopRate]`에 토픽별 `rx`·`ros_publish` Hz와 실제 수신/발행 수,
  합친 수·만료 수를 기록합니다. `rx`도 낮으면 송신 앱·네트워크를, `rx`는100인데 ROS 발행만 낮으면
  Endpoint 처리·밀림을 확인합니다. ROS 발행은 구독자의 실제 수신 보장과 구분하며, 무수신 중에
  가짜 상태나 heartbeat를 만들지 않습니다.

격리된 실제 TCP→ROS 구독자 시험에서 100Hz·200개 샘플을 보냈습니다.

| 도착 방식 | 기존 실제 수신 | 수정 후 실제 수신 |
|---|---:|---:|
| 한 개씩 | 100.01Hz · 200개 | 100.01Hz · 200개 |
| 두 개씩 | 50.01Hz · 100개 | 100.51Hz · 200개 |
| 세 개씩 | 33.51Hz · 67개 | 101.03Hz · 200개 |
| 네 개씩 | 25.00Hz · 50개 | 101.52Hz · 200개 |
| 1·1·2·1개씩 반복 | 79.90Hz · 160개 | 100.01Hz · 200개 |

묶음의 처음/마지막 시각 때문에 짧은 관측 창에서 Hz가100보다 조금 크게 계산될 수 있지만
모든 경우 원본200개만 발행했습니다. 개발 PC의 loopback 재현이며 실기 무선망의 보장 수치가 아닙니다.
PR85는 ASH 웹/호스트의 권한 검사 변경이며 TCP 전송 코드를 바꾸지 않았습니다. 이번 재현은
Endpoint의 별도 병목을 확인한 것이고 실제 무선 손실·HMD 송신률은 실기에서 다시 측정해야 합니다.

변경은 Endpoint 프로세스를 다시 실행해야 반영됩니다. 일반 설치본은 사용자 `rs`·`cbo` 빌드가
필요하며, 이 작업에서는 ROS 빌드·실기 Endpoint/ASH 재시작·무선 장치 설정 변경을 하지 않았습니다.

최종 검증은 TCP 기능59개와 실제 M2 노드를 격리 도메인에서 실행한 입력 손실/명시적 재시작 시험1개가
통과했습니다. 기존 코드에 같은100Hz 시험을 적용하면 묶음 입력4개 조건에서 샘플 손실이 재현됩니다.

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

## CPU forwarding optimization (2026-10-13)

Topic subscribers receive raw ROS CDR bytes and wrap them in the existing TCP
frame. Normal CDR1 little-endian data avoids the Python message allocation and
deserialization/serialization cycle. Other encapsulations retain the previous
typed conversion to preserve Unity's wire layout. Topic types, QoS, frequency
policies, latest-only reconnect snapshots, service messages and input handling
remain unchanged. Raw subscriptions are supported by the
[Humble rclpy implementation](https://github.com/ros2/rclpy/blob/humble/rclpy/rclpy/node.py).

Queued topics check their configured throttle before serializing. With no client,
queued data is discarded before serialization; latest-only data still populates
its reconnect cache. No new throttle is introduced.

On the development PC (x86_64, Python 3.12.3), median CPU time for CDR conversion
and TCP framing of an RGB8 image changed from 0.503 to 0.019 ms at 640×360 and
1.120 to 0.145 ms at 1280×720. The framed bytes were identical. These timings
exclude DDS, executor scheduling, networking and actual robot process load.

The transport and raw forwarding suites passed 36 tests, including actual ROS
image/scan/TF delivery, byte-identical payloads and big-endian normalization:

```sh
ROS_DOMAIN_ID=196 ROS_LOCALHOST_ONLY=1 PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
  python3 -m pytest -q test/test_framed_input.py test/test_input_transport.py \
  test/test_outbound_latency.py test/test_raw_forwarding.py
```

Restart the rebuilt endpoint to load the changes and compare CPU and delivery
gaps on the robot with the same connected client and traffic.

## License

[Apache License 2.0](LICENSE)
