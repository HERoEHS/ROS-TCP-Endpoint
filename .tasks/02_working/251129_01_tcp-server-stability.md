## TCP Server 안정성 개선 및 메모리 최적화
- **Start Date**:
- **End Date**:
- **Assignee**:
- **Description**: ROS-TCP-Endpoint 서버의 예외 처리 강화, Race condition 해결, 고주파 토픽 메모리 문제 해결
- **Tag**: `#ros2` `#python` `#bugfix` `#refactor` `#medium`
- **State**: TODO
- **Details**:
  - [Details: TCP Server Stability](./251129_01_tcp-server-stability_detail.md)

### Checklist

#### Critical 수정 (기존) ✅
- [x] [Agent] `handle_syscommand`의 `getattr` 기본값 추가 (AttributeError 방지)
- [x] [Agent] `handle_syscommand`에 JSON 파싱 예외 처리 추가
- [x] [Agent] `SysCommand_*` 클래스 인스턴스 변수 수정 (`self.` 누락)
- [x] [Agent] `executor` 속성 `__init__`에서 초기화

#### Critical 수정 (추가 발견) ✅
- [x] [Agent] `publisher.py` deserialize 주석 해제 및 수정 (현재 동작 안 함)
- [x] [Agent] `client.py:194` `read_message()` None 반환 시 처리 추가
- [x] [Agent] `client.py:104` `self.logerr` → `self.tcp_server.logerr` 수정
- [x] [Agent] `service.py` busy-wait → 타임아웃 + sleep 방식으로 변경

#### Race Condition 해결
- [ ] 테이블 동시 접근 락(Lock) 추가 여부 검토
- [ ] [Agent] 필요 시 `publishers_table`, `subscribers_table` 등에 threading.Lock 적용
- [x] [Agent] `pending_srv_id` 멀티 클라이언트 race condition 해결 (클라이언트별 분리)

#### 멀티 클라이언트 지원
- [ ] 현재 단일 클라이언트 제한 구조 분석 (`self.queue` 단일 참조)
- [ ] [Agent] 클라이언트별 큐 관리 구조로 변경 (`self.queues` 딕셔너리/리스트)
- [ ] [Agent] 클라이언트 연결/해제 시 큐 등록/제거 로직 추가
- [ ] 다수 클라이언트 동시 연결 테스트

#### 메모리 최적화 (고주파 토픽) - Latest-Only + 설정 파일 방식
- [ ] [Agent] `config/topic_policy.yaml` 설정 파일 구조 생성
- [ ] [Agent] `tcp_sender.py`에 설정 파일 로드 기능 추가
- [ ] [Agent] 토픽별 정책 분기 구현 (`latest_only` vs `queue`)
- [ ] [Agent] 선택적 스로틀링 (`max_frequency`) 구현
- [ ] [Agent] launch 파라미터로 설정 파일 경로 지정 기능
- [ ] 설정 파일 예시 작성 및 문서화

#### 안정성 개선 (Medium) ✅
- [x] [Agent] `thread_pauser.py` 서비스 타임아웃 추가 (영구 블록 방지)
- [x] [Agent] `unity_service.py` unregister()에 `destroy_service()` 추가
- [x] [Agent] `server.py` resolve_message_name getattr 예외 처리 수정
- [x] [Agent] `server.py` socket.bind() 예외 처리 추가 (포트 충돌)
- [x] [Agent] `client.py` 메시지 크기 제한 추가 (메모리 공격 방지)

#### 개선 권장 (Low)
- [x] [Agent] TCP 소켓 최적화 (`TCP_NODELAY`, `SO_KEEPALIVE`)
- [ ] [Agent] Graceful shutdown 구현 (연결 정리, 소켓 close)
- [ ] [Agent] 노드 이름 충돌 방지 (유니크 suffix 추가)
- [x] [Agent] `subscriber.py:56` 의미 없는 코드 제거

#### 테스트 및 검증
- [ ] 고주파 토픽 (100Hz+) 다수 구독 시 메모리 사용량 테스트
- [ ] 비정상 입력 (잘못된 JSON, 없는 토픽 등) 예외 처리 테스트
- [ ] Unity 연결/해제 반복 시 안정성 테스트
- [ ] 멀티 클라이언트 동시 서비스 호출 테스트
- [ ] 대용량 메시지 전송 테스트

### Notes
- 데이터 누락 방지와 메모리 최적화는 트레이드오프 관계이므로 용도에 따라 정책 분리 필요
- 실시간 센서 데이터는 최신 값만 중요 → 드롭 허용
- 명령/이벤트 데이터는 손실 불가 → 큐 유지 또는 재전송 메커니즘 필요
- 현재 마지막 연결 클라이언트만 메시지 수신 가능 → 멀티 클라이언트 지원 필요
