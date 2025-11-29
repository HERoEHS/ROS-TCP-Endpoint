# Format

## Example Task Format (Make to Markdown file)

## Task Name
- **Start Date**: YYYY-MM-DD // Write the date when task moves 01 -> 02
- **End Date**: YYYY-MM-DD // Write the date when task moves 02 -> 03
- **Assignee**:
- **Description**: Write a short description of the task
- **Tag**: Write tags in the format of `#tag_name`
- **State**: TODO // TODO, IN PROGRESS, DONE
- **Details**:
  - [Details: ??](./task_details/2025-00-00_detail-template.md)
  - [Knowledge: ??](./knowledge/2025-00-00_knowledge-template.md)

### Checklist
- [ ] TODO 1
- [ ] TODO 2

---

# Rules

## Workflow
- **현재까지의 작업 이해**: `.tasks/05_summary/`를 참고하여 현재까지의 작업을 이해합니다.
- **Task 생성**: 새로운 작업을 `01_planning` 폴더에 있는 `250101_00_task_template.md`를 읽고 `YYMMDD_NN_task-name.md` 형식의 파일로 생성합니다. details 파일은 `Details Template`을 참고하여 작성합니다.
- **Task 시작**: 작업을 시작할 때, 해당 파일을 `01_planning` 폴더에서 `02_working` 폴더로 이동시킵니다.
- **Task 완료**: 작업이 완료되면, 해당 파일을 `02_working` 폴더에서 `03_finished` 폴더로 이동시킵니다.

## Agent 활용
- 02_working의 작업을 agent에게 요청할 때는 Details 파일과 함께 컨텍스트 제공
- agent는 Checklist 항목을 보고 자동화 가능한 부분 제안 가능
- 코드 생성/리팩터링은 agent, 실행/테스트/검증은 사람이 담당

## 완료 기준
- 모든 Checklist 항목이 체크되었을 때
- 실제 동작 확인 또는 테스트 통과했을 때
- Details 파일에 "진행 로그"와 "메모"를 작성했을 때
- 위 조건 만족 시 02 → 03으로 이동

## Tags
- Write tags in the format of `#tag_name`
- tags should describe the technical aspect of the task
- 예시:
  - 기술: `#unity`, `#ros2`, `#react`, `#python`, `#csharp`
  - 유형: `#arch`, `#bugfix`, `#feature`, `#refactor`, `#docs`, `#test`
  - 영역: `#frontend`, `#backend`, `#ui`, `#infra`, `#robot`
  - 난이도: `#easy`, `#medium`, `#hard`

## State
- 01_planning => State = TODO
- 02_working => State = IN PROGRESS
- 03_finished => State = DONE

## Details 파일
- 작업이 복잡하거나 설계/로그가 필요한 경우 별도 `YYMMDD_NN_task-name_detail.md` 파일 생성
- 파일명 형식: `YYMMDD_NN_task-name_detail.md` (예: `251119_00_ros-tcp-endpoint_detail.md`)
- 간단한 작업(1~2시간)은 details 파일 생략 가능
- Details 파일은 `Details Template`을 참고하여 작성
- 파일 여러개를 사용할 경우 `Details` 항목에 모두 추가

## Knowledge & Documentation 관리
- 재사용 가능성이 높은 정보, 아키텍처 결정, 기술 가이드 등은 프로젝트 루트의 `docs` 폴더에서 관리
- 폴더 구조:
  - `docs/architecture/`: 아키텍처 결정, 구조 설명
  - `docs/guide/`: 개발 가이드, 패턴 설명
  - `docs/style_guide/`: 코드/UI 스타일 가이드
  - `docs/troubleshooting/`: 트러블슈팅 기록
- 문서 작성 후 `docs/index.md`의 목차에 추가하여 접근성 확보
- 작업 중 얻게 된 지식이나 노하우를 기록하고 공유하는 것을 목적으로 함

## Checklist 작성
- 각 항목은 1~2시간 내 완료 가능한 단위로 쪼개기
- 완료 조건이 명확하게 적기 (예: ✅ "BoxModel 클래스 작성" ❌ "모델 작업")
- agent에게 맡길 부분은 `[Agent]` 표시 (예: `[ ] [Agent] 클래스 스켈레톤 생성`)
