# TCP Server 안정성 개선 및 메모리 최적화

## 개요
- **목표**: ROS-TCP-Endpoint 서버의 예외 처리 강화, 동시성 문제 해결, 고주파 토픽 메모리 폭발 방지
- **배경/맥락**: 코드 리뷰 결과 여러 잠재적 크래시 포인트와 고주파 토픽 처리 시 메모리 급증 문제 발견
- **작업 유형**: #bugfix #refactor

---

## 문제 / 해결

### 🔴 Critical 문제들

#### 1. `handle_syscommand` AttributeError
- **파일**: `server.py:120-127`
- **증상**: 잘못된 syscommand 수신 시 서버 크래시
- **원인**: `getattr()`에 기본값 없음 → 없는 속성 접근 시 `AttributeError`
- **해결**:
```python
function = getattr(self.syscommands, topic[2:], None)
```

#### 2. JSON 디코딩 예외 미처리
- **파일**: `server.py:125-126`
- **증상**: 잘못된 JSON 데이터 수신 시 연결 처리 실패
- **원인**: `json.loads()` 예외 처리 없음
- **해결**:
```python
try:
    message_json = data.decode("utf-8")[:-1]
    params = json.loads(message_json)
    function(**params)
except (json.JSONDecodeError, UnicodeDecodeError) as e:
    self.send_unity_error(f"Invalid syscommand data: {e}")
```

#### 3. `SysCommand_*` 클래스 버그
- **파일**: `tcp_sender.py:214-227`
- **증상**: 직렬화 시 `AttributeError`
- **원인**: `self.` 누락으로 인스턴스 변수가 아닌 로컬 변수로 생성됨
- **해결**:
```python
class SysCommand_Log:
    def __init__(self):
        self.text = ""  # self. 추가
```

#### 4. `executor` 미초기화
- **파일**: `server.py`
- **증상**: `setup_executor()` 호출 전 `unregister_node()` 호출 시 `AttributeError`
- **해결**: `__init__`에 `self.executor = None` 추가

#### 5. `publisher.py` deserialize 누락 (동작 안 함)
- **파일**: `publisher.py:54-57`
- **증상**: Unity→ROS publish가 동작하지 않음
- **원인**: deserialize 코드가 주석 처리됨
- **해결**:
```python
def send(self, data):
    message_type = type(self.msg)
    message = deserialize_message(data, message_type)
    self.pub.publish(message)
    return None
```

#### 6. `client.py` read_message None 반환 시 크래시
- **파일**: `client.py:103-108, 194`
- **증상**: 메시지 읽기 실패 시 `TypeError: cannot unpack non-iterable NoneType`
- **원인**: `read_message()`가 None 반환 가능하나 호출부에서 미처리
- **해결**:
```python
result = self.read_message(self.conn)
if result is None:
    break
destination, data = result
```

#### 7. `client.py` 존재하지 않는 메서드 호출
- **파일**: `client.py:104`
- **증상**: `AttributeError: 'ClientThread' object has no attribute 'logerr'`
- **원인**: `self.logerr()` 사용하지만 메서드 없음
- **해결**: `self.tcp_server.logerr()` 로 수정

#### 8. `service.py` CPU 100% busy-wait
- **파일**: `service.py:65-73`
- **증상**: 서비스 호출 시 CPU 사용량 급증
- **원인**: `while rclpy.ok()` 루프에서 sleep 없이 polling
- **해결**:
```python
def send(self, data):
    message = deserialize_message(data, type(self.req))
    if not self.cli.service_is_ready():
        self.get_logger().error(f"Service {self.service_topic} not ready")
        return None

    future = self.cli.call_async(message)
    rclpy.spin_until_future_complete(self, future, timeout_sec=10.0)

    if future.done():
        return future.result()
    return None
```

---

### 🟠 Medium 문제들

#### 9. `pending_srv_id` 멀티 클라이언트 Race Condition
- **파일**: `server.py:76-77`
- **증상**: 멀티 클라이언트 환경에서 서비스 응답이 잘못된 클라이언트로 전달
- **원인**: `pending_srv_id`가 서버 전역 변수로 공유됨
- **해결**: 클라이언트별로 분리하거나 메시지에 srv_id 포함

#### 10. 서비스 타임아웃 없음
- **파일**: `thread_pauser.py:8-10`
- **증상**: Unity 응답이 없으면 ROS 서비스 스레드 영구 블록
- **해결**:
```python
def sleep_until_resumed(self, timeout=30.0):
    with self.condition:
        return self.condition.wait(timeout=timeout)
```

#### 11. `unity_service.py` service destroy 누락
- **파일**: `unity_service.py:59-65`
- **해결**:
```python
def unregister(self):
    self.destroy_service(self.service)
    self.destroy_node()
```

#### 12. Socket bind 예외 처리 없음
- **파일**: `server.py:97`
- **증상**: 포트가 이미 사용 중이면 크래시
- **해결**:
```python
try:
    tcp_server.bind((self.tcp_ip, self.tcp_port))
except OSError as e:
    self.logerr(f"Failed to bind {self.tcp_ip}:{self.tcp_port} - {e}")
    return
```

#### 13. 메시지 크기 제한 없음
- **파일**: `client.py:99-101`
- **증상**: 악의적/잘못된 거대 메시지로 메모리 공격 가능
- **해결**:
```python
MAX_MESSAGE_SIZE = 100 * 1024 * 1024  # 100MB
full_message_size = ClientThread.read_int32(conn)
if full_message_size > MAX_MESSAGE_SIZE:
    raise ValueError(f"Message too large: {full_message_size}")
```

---

### 🟢 Low 개선사항

#### 14. TCP 소켓 최적화
```python
conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)  # 지연 최소화
conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)  # 연결 감지
```

#### 15. 노드 이름 충돌 방지
```python
import uuid
node_name = f"{strippedTopic}_RosSubscriber_{uuid.uuid4().hex[:8]}"
```

#### 16. 의미 없는 코드 제거
- **파일**: `subscriber.py:56`
- `self.subscription` 단독 라인 삭제

---

### 🟡 멀티 클라이언트 지원

#### 현재 구조 문제
- **파일**: `tcp_sender.py:177-178`
- **증상**: 마지막 연결 클라이언트만 메시지 수신, 이전 클라이언트는 메시지 수신 불가
- **원인**: `self.queue`가 단일 참조로, 새 클라이언트 연결 시 덮어씀
- **현재 동작**:
```
A 클라이언트 연결 → self.queue = A의 local_queue
B 클라이언트 연결 → self.queue = B의 local_queue
                    ↓
          A는 더 이상 메시지 수신 불가 (연결은 유지)
```

#### 해결 방안
```python
# tcp_sender.py
class UnityTcpSender:
    def __init__(self, tcp_server):
        self.queues = {}  # {client_id: Queue}
        self.queues_lock = threading.Lock()

    def send_unity_message(self, topic, message):
        serialized_message = ClientThread.serialize_message(topic, message)
        with self.queues_lock:
            for queue in self.queues.values():
                queue.put(serialized_message)

    def register_client(self, client_id, queue):
        with self.queues_lock:
            self.queues[client_id] = queue

    def unregister_client(self, client_id):
        with self.queues_lock:
            if client_id in self.queues:
                del self.queues[client_id]
```

---

### 🟠 메모리 문제 (고주파 토픽)

#### 문제 분석
- **파일**: `tcp_sender.py:170`
- **증상**: 고주파 토픽 다수 구독 시 메모리 급증
- **원인**: `Queue()`가 무제한 크기, TCP 전송 속도 < ROS 토픽 수신 속도
- **데이터 흐름**:
```
ROS Topic (1000Hz) → Subscriber.send() → queue.put() → sender_loop → TCP
      ↓                    ↓                 ↓              ↓
   빠름              즉시 큐 추가       무제한 쌓임      느림 (~500msg/s)
```

#### 방향별 특성 분석

```
ROS → Unity (확인용)     : 드롭 허용, 최신 값이 중요
Unity → ROS (조종 명령)  : 현재 동기 처리로 손실 없음 ✅
```

**결론**: 메모리 문제는 ROS→Unity 방향에서만 발생, 드롭 허용 가능

#### 권장 구현: Latest-Only + 서버 설정 파일

##### 설정 파일 (`config/topic_policy.yaml`)
```yaml
# 토픽별 전송 정책 설정
# policy: "latest_only" | "queue" (기본값: queue)
# max_frequency: Hz 단위 스로틀링 (선택, 0 = 무제한)

topic_policies:
  # 고주파 센서 - 최신 값만 유지
  /joint_states:
    policy: latest_only
    max_frequency: 100  # 100Hz로 제한

  /camera/image_raw:
    policy: latest_only
    max_frequency: 30

  /scan:
    policy: latest_only
    max_frequency: 20

  # 이벤트성 토픽 - 큐 유지 (손실 방지)
  /goal_reached:
    policy: queue

  /button_event:
    policy: queue

# 기본 정책 (설정에 없는 토픽)
default_policy:
  policy: queue
  max_queue_size: 100
```

##### 구현 설계
```python
# tcp_sender.py
class UnityTcpSender:
    def __init__(self, tcp_server, config_path=None):
        self.queues = {}           # 멀티 클라이언트용
        self.latest_messages = {}  # {topic: serialized_message}
        self.topic_policies = {}   # 설정에서 로드
        self.last_send_time = {}   # 스로틀링용

        if config_path:
            self._load_config(config_path)

    def _load_config(self, path):
        """YAML 설정 파일 로드"""
        with open(path, 'r') as f:
            config = yaml.safe_load(f)
        self.topic_policies = config.get('topic_policies', {})
        self.default_policy = config.get('default_policy', {'policy': 'queue'})

    def _get_policy(self, topic):
        """토픽별 정책 반환 (와일드카드 패턴 지원 가능)"""
        return self.topic_policies.get(topic, self.default_policy)

    def send_unity_message(self, topic, message):
        policy = self._get_policy(topic)
        serialized = ClientThread.serialize_message(topic, message)

        # 스로틀링 체크
        max_freq = policy.get('max_frequency', 0)
        if max_freq > 0:
            now = time.time()
            min_interval = 1.0 / max_freq
            if now - self.last_send_time.get(topic, 0) < min_interval:
                return  # 스킵
            self.last_send_time[topic] = now

        if policy.get('policy') == 'latest_only':
            # 최신 값만 유지 (이전 값 덮어씀)
            with self.message_lock:
                self.latest_messages[topic] = serialized
        else:
            # 큐에 추가 (기존 방식)
            self._enqueue_to_all_clients(serialized)

    def sender_loop(self, conn, client_id, halt_event):
        """주기적으로 latest_messages와 큐 모두 전송"""
        while not halt_event.is_set():
            # 1. Latest-only 토픽들 전송
            with self.message_lock:
                latest = dict(self.latest_messages)
                self.latest_messages.clear()

            for msg in latest.values():
                conn.sendall(msg)

            # 2. 큐 메시지 전송
            # ... 기존 로직
```

##### 설정 파일 위치 옵션
```
옵션 1: 패키지 내부
  ros_tcp_endpoint/
    config/
      topic_policy.yaml

옵션 2: launch 파라미터로 지정
  ros2 launch ros_tcp_endpoint endpoint.launch.py \
    config_file:=/path/to/topic_policy.yaml

옵션 3: ROS 파라미터로 지정
  ros2 run ros_tcp_endpoint default_server_endpoint \
    --ros-args -p topic_config:=/path/to/config.yaml
```

#### 메모리 사용량 비교

| 방식               | 100개 토픽 × 1000Hz | 메모리     |
| ------------------ | ------------------- | ---------- |
| 현재 (무제한 큐)   | 10초 = 1M 메시지    | 수백 MB~GB |
| **Latest-Only**    | 최대 100개 메시지   | **수 MB**  |
| 큐 제한 (100/토픽) | 최대 10K 메시지     | 수십 MB    |

---

## 기술 스택 / 도구
- **언어**: Python 3
- **프레임워크**: ROS 2 (rclpy)
- **라이브러리**: threading, queue, socket
- **테스트**: 고주파 토픽 생성 노드, memory_profiler

---

## Agent 역할 & 사람 역할
- **Agent**:
  - Critical 버그 수정 코드 생성
  - 메모리 최적화 코드 작성
  - 테스트 스크립트 생성
- **사람**:
  - 데이터 손실 vs 메모리 트레이드오프 결정
  - 실제 환경 테스트 및 검증
  - 토픽별 정책 최종 결정

---

## 진행 로그
- `15:41` – 코드 리뷰 시작, Critical 이슈 발견
- `15:48` – 고주파 토픽 메모리 문제 분석 완료
- `15:51` – 태스크 플래닝 문서 작성
- `16:00` – 멀티 클라이언트 지원 요구사항 추가
- `16:19` – 메모리 최적화 방안 심층 분석 (Latest-Only + 서버 설정 파일 방식 채택)
- `16:27` – 추가 코드 분석, 총 16개 문제 발견 (Critical 8, Medium 5, Low 3)
