# Priority Queue Project

## 개요

이 프로젝트는 Raft 합의 알고리즘을 사용하여 분산 환경에서 우선순위 큐를 구현하는 예제 제공을 목표로 합니다.  `com.example.priorityqueue` 패키지를 기반으로 구축되었으며, 우선순위 큐에 대한 요청을 처리하고 스냅샷을 통해 상태를 저장 및 복원할 수 있는 기능을 제공합니다.

## 구조

프로젝트의 주요 패키지 및 클래스는 다음과 같습니다.
```
jraft-priority-queue/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/
│   │   │       └── example/
│   │   │           └── priorityqueue/
│   │   │               ├── server/
│   │   │               │   ├── PriorityQueueServer.java
│   │   │               │   └── PriorityQueueStateMachine.java
│   │   │               ├── client/
│   │   │               │   └── PriorityQueueClient.java
│   │   │               ├── common/
│   │   │               │   ├── Element.java
│   │   │               │   ├── OperationType.java
│   │   │               │   └── PriorityQueueClosure.java
│   │   │               │   └── PriorityQueueRequest.java 
│   │   │               │   └── PriorityQueueRequestProcessor.java 
│   │   │               │   └── PriorityQueueResponse.java
│   │   │               │   └── Utils.java
│   │   │               ├── ServerMain.java
│   │   │               └── ClientMain.java
│   │   └── resources/
│   │       └── log4j.properties
```

*   **`com.example.priorityqueue.common`**: 우선순위 큐 요청 및 요소와 관련된 공통 클래스를 포함합니다.
*   **`com.example.priorityqueue.server`**: 우선순위 큐의 서버 측 구현을 포함합니다.
*   **`com.example.priorityqueue.client`**: 우선순위 큐 클라이언트 측 구현을 포함합니다.

## 주요 클래스 설명

*   **`com.example.priorityqueue.common.PriorityQueueRequest`**:
    *   우선순위 큐에 대한 요청을 나타냅니다.
    *   `OperationType` (요청 유형) 및 `Element` (요청과 관련된 요소)를 포함합니다.
    *   `deserialize(byte[] bytes)` 메서드를 사용하여 바이트 배열에서 역직렬화할 수 있습니다.
*   **`com.example.priorityqueue.server.PriorityQueueStateMachine`**:
    *   우선순위 큐의 상태 머신을 구현합니다. `StateMachineAdapter`를 확장합니다.
    *   내부적으로 `java.util.PriorityQueue`를 사용하여 우선순위 큐를 저장합니다.
    *   Raft 로그 적용(`onApply`), 스냅샷 저장(`onSnapshotSave`), 스냅샷 로드(`onSnapshotLoad`) 기능을 제공합니다.
    *   `peek()` 메서드를 통해 큐의 head에 있는 element를 반환합니다.
*   **`Element`**: 우선순위 큐에 저장될 요소를 나타내는 클래스입니다. 우선순위 값을 가지고 있을 것으로 예상됩니다.
*   **`OperationType`**: 수행할 작업 유형을 나타내는 Enum 입니다. 예: 삽입, 삭제 등.

## 데이터 흐름

1.  클라이언트는 `PriorityQueueRequest` 객체를 직렬화하여 서버로 보냅니다.
2.  서버는 `PriorityQueueRequest.deserialize()`를 사용하여 요청을 역직렬화합니다.
3.  `PriorityQueueStateMachine`은 수신된 요청을 기반으로 raft 상태를 업데이트하고, 우선순위 큐에 대한 작업을 수행합니다.
4.  `PriorityQueueStateMachine`은 정기적으로 큐의 상태를 스냅샷으로 저장합니다 (`onSnapshotSave`).
5.  필요한 경우 `PriorityQueueStateMachine`은 스냅샷에서 상태를 복원할 수 있습니다 (`onSnapshotLoad`).

## 사용법
- 아래 실행 부분을 참고해, 먼저 3개 서버 노드를 실행 합니다.
- 아래 실행 부분을 참고해, 클라이언트를 실행 합니다.
- 클라이언트가 지원하는 커맨드(enqueue, dequeue, peek) 를 실행 합니다.
   - `enqueue <value> <priority>`: 우선순위 큐에 요소 추가
   - `dequeue`: 가장 높은 우선순위의 요소 제거 및 반환
   - `peek`: 가장 높은 우선순위의 요소 확인 (제거하지 않음)
   - `quit`: 클라이언트 종료
- HA 동작 확인을 위해 서버 노드를 강제 종료, 재시작등을 수행합니다.
### 환경
- java 11
- maven
### 빌드
> mvn clean package
### 실행
- 서버
  - ##### 1번 노드
  > java --add-opens java.base/java.lang=ALL-UNNAMED -jar target/priority-queue-server.jar /tmp/server1 priority_queue_group 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
  - ##### 2번 노드
  > java --add-opens java.base/java.lang=ALL-UNNAMED -jar target/priority-queue-server.jar /tmp/server2 priority_queue_group 127.0.0.1:8082 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
  - ##### 3번 노드
  > java --add-opens java.base/java.lang=ALL-UNNAMED -jar target/priority-queue-server.jar /tmp/server3 priority_queue_group 127.0.0.1:8083 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
- 클라이언트
  > java --add-opens java.base/java.lang=ALL-UNNAMED -jar target/priority-queue-client.jar priority_queue_group 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083
 
