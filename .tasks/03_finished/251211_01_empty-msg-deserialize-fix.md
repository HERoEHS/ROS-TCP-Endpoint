# Empty 메시지 역직렬화 수정

- **Start Date**: 2025-12-10
- **End Date**: 2025-12-11
- **Assignee**: Cascade
- **Description**: Unity ROS-TCP-Connector에서 보내는 Empty 타입 메시지가 ROS-TCP-Endpoint에서 역직렬화 실패하는 문제 수정
- **Tag**: `#ros2` `#unity` `#bugfix` `#python` `#csharp`
- **State**: DONE

## 문제

Unity에서 `std_msgs/Empty`, `std_srvs/Trigger` 같은 Empty 타입 메시지를 보내면 ROS-TCP-Endpoint에서 다음 에러 발생:

```
rclpy._rclpy_pybind11.RMWError: failed to deserialize ROS message:
rmw_serialize: invalid data size, at ./src/rmw_node.cpp:1861
```

## 원인 분석

### 데이터 흐름

```
Unity (ROS-TCP-Connector)
         │
         │  CDR 헤더(4 bytes) + payload 전송
         ▼
ROS2 (ROS-TCP-Endpoint)
         │
         │  rclpy.deserialize_message()
         ▼
ROS2 네트워크
```

### 핵심 발견

1. **rclpy.deserialize_message**는 **CDR 헤더를 포함한 데이터**를 기대함
2. Empty 타입은 payload가 없어서 **CDR 헤더(4바이트: `0x00 0x01 0x00 0x00`)만** 전송됨
3. rclpy는 4바이트만 있는 데이터를 역직렬화하면 에러 발생

### 메시지 타입별 데이터 구조

| 메시지 타입 | Unity가 보내는 데이터 | 크기        |
| ----------- | --------------------- | ----------- |
| Empty       | CDR 헤더만            | 4 bytes     |
| 일반 메시지 | CDR 헤더 + payload    | 4 + N bytes |

## 해결책

Empty 메시지(CDR 헤더만 있는 경우)는 `deserialize_message` 호출 없이 직접 인스턴스 생성:

```python
# publisher.py / service.py
if data == b'\x00\x01\x00\x00':  # Empty (CDR 헤더만)
    message = message_type()     # 직접 생성
else:
    message = deserialize_message(data, message_type)  # 정상 역직렬화
```

## 수정된 파일

### ROS-TCP-Endpoint (Python)

- `ros_tcp_endpoint/publisher.py` - `send()` 메서드
- `ros_tcp_endpoint/service.py` - `send()` 메서드

### ROS-TCP-Connector (Unity C#)

- `MessageSerializer.cs` - `#if ROS2` 제거, 항상 ROS2 CDR 포맷 사용
- `MessageDeserializer.cs` - `#if ROS2` 제거, 항상 ROS2 CDR 포맷 사용

### Unity 프로젝트 설정

- `Packages/manifest.json` - 로컬 ROS-TCP-Connector 경로로 변경:
  ```json
  "com.unity.robotics.ros-tcp-connector": "file:/home/yh/git_ws/src/ROS-TCP-Connector/com.unity.robotics.ros-tcp-connector"
  ```

## 관련 이슈

- [Unity-Robotics-Hub #390](https://github.com/Unity-Technologies/Unity-Robotics-Hub/issues/390) - Trigger service call crashes ros_tcp_endpoint Node

### Checklist

- [x] 문제 원인 분석
- [x] publisher.py Empty 메시지 처리 수정
- [x] service.py Empty 요청 처리 수정
- [x] MessageSerializer.cs ROS2 CDR 포맷 고정
- [x] MessageDeserializer.cs ROS2 CDR 포맷 고정
- [x] Unity 프로젝트 manifest.json 로컬 경로 설정
- [x] 테스트 완료

---
