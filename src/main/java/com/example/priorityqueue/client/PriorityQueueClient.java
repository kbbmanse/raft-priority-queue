package com.example.priorityqueue.client;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RouteTable;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.RpcResponseClosureAdapter;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;
import com.example.priorityqueue.common.Element;
import com.example.priorityqueue.common.OperationType;
import com.example.priorityqueue.common.PriorityQueueRequest;
import com.example.priorityqueue.common.PriorityQueueResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.example.priorityqueue.common.Utils.deserialize;
import static com.example.priorityqueue.common.Utils.serialize;

public class PriorityQueueClient {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueClient.class);

    private final CliClientServiceImpl cliClientService; // jraft 클라이언트 서비스 구현체
    private final String groupId; // raft 그룹 ID
    private PeerId leaderId; // 현재 리더의 ID
    private final int rpcTimeoutMs = 5000; // RPC 타임아웃 (밀리초)

    public PriorityQueueClient(String groupId) {
        this.cliClientService = new CliClientServiceImpl(); // 클라이언트 서비스 구현체 초기화
        this.cliClientService.init(new CliOptions()); // 클라이언트 옵션 초기화
        this.groupId = groupId; // 그룹 ID 설정
    }

    /**
     * Raft 그룹에 참여하는 피어(Peer)들을 추가합니다.
     *
     * @param confStr 피어들의 정보를 담고 있는 문자열 (예: "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083")
     */
    public void addPeers(String confStr) {
        RouteTable.getInstance().updateConfiguration(groupId, JRaftUtils.getConfiguration(confStr)); // RouteTable에 피어 정보를 업데이트
        refreshLeader(); // 리더 정보를 갱신
    }

    /**
     * 우선순위 큐에 대한 특정 연산(enqueue, dequeue, peek)을 수행하고 결과를 반환합니다.
     *
     * @param type    수행할 연산 타입 (OperationType enum)
     * @param element 연산에 필요한 요소 (enqueue의 경우 추가할 요소, dequeue/peek의 경우 null)
     * @param <T>     반환 타입 (enqueue는 boolean, dequeue/peek는 Element)
     * @return 연산 결과
     * @throws Exception 연산 실패 시 예외 발생
     */
    @SuppressWarnings("unchecked")
    private <T> T processOperation(OperationType type, Element element) throws Exception {
        if (!refreshLeader()) {
            throw new IllegalStateException("Leader not available"); // 리더가 없으면 예외 발생
        }

        // 요청 객체를 직렬화
        final BytesValue msg = BytesValue.of(ByteString.copyFrom(serialize(new PriorityQueueRequest(type, element))));
        CompletableFuture<T> future = new CompletableFuture<>(); // 비동기 결과를 처리하기 위한 CompletableFuture 생성

        // RPC 호출 결과를 처리하기 위한 ClosureAdapter 생성
        RpcResponseClosureAdapter<BytesValue> closureAdapter = new RpcResponseClosureAdapter<>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) { // RPC 호출 성공 시
                    BytesValue response = getResponse(); // 응답 획득
                    byte[] responseData = response.getValue().toByteArray(); // 응답 데이터 획득
                    PriorityQueueResponse<?> queueResponse = (PriorityQueueResponse<?>) deserialize(responseData); // 응답 데이터 역직렬화
                    future.complete((T) queueResponse.getResult()); // future에 결과를 설정
                } else { // RPC 호출 실패 시
                    future.completeExceptionally(new Throwable(status.getErrorMsg())); // future에 예외를 설정
                }
            }
        };

        // 리더에게 RPC 호출
        cliClientService.invokeWithDone(
                leaderId.getEndpoint(), // 리더의 엔드포인트
                msg,     // Message 타입으로 래핑된 객체
                closureAdapter, // 결과를 처리할 ClosureAdapter
                rpcTimeoutMs // RPC 타임아웃
        );

        // closure에서 결과 대기 및 반환
        return future.get(rpcTimeoutMs, TimeUnit.MILLISECONDS); // future에서 결과를 얻어 반환 (타임아웃 설정)
    }

    /**
     * 우선순위 큐에 요소를 삽입합니다.
     *
     * @param value    요소의 값
     * @param priority 요소의 우선순위
     * @return 삽입 성공 여부
     * @throws Exception 연산 실패 시 예외 발생
     */
    public boolean enqueue(String value, int priority) throws Exception {
        return processOperation(OperationType.ENQUEUE, new Element(value, priority)); // enqueue 연산 수행
    }

    /**
     * 우선순위 큐에서 가장 높은 우선순위를 가진 요소를 삭제하고 반환합니다.
     *
     * @return 삭제된 요소
     * @throws Exception 연산 실패 시 예외 발생
     */
    public Element dequeue() throws Exception {
        return processOperation(OperationType.DEQUEUE, null); // dequeue 연산 수행
    }

    /**
     * 우선순위 큐에서 가장 높은 우선순위를 가진 요소를 반환합니다 (삭제하지 않음).
     *
     * @return 큐의 head 에 있는 요소
     * @throws Exception 연산 실패 시 예외 발생
     */
    public Element peek() throws Exception {
        return processOperation(OperationType.PEEK, null); // peek 연산 수행
    }

    /**
     * 클라이언트 서비스를 종료합니다.
     */
    public void shutdown() {
        this.cliClientService.shutdown(); // 클라이언트 서비스 종료
    }

    /**
     * Raft 그룹의 리더를 갱신합니다.
     *
     * @return 리더 갱신 성공 여부
     */
    private boolean refreshLeader() {
        try {
            // RouteTable을 사용하여 리더를 갱신
            Status status = RouteTable.getInstance()
                    .refreshLeader(this.cliClientService, this.groupId, 1000);
            if (!status.isOk()) { // 갱신 실패 시
                LOG.error("Failed to refresh leader: {}", status);
                return false; // 실패 반환
            }
            this.leaderId = RouteTable.getInstance().selectLeader(this.groupId); // 리더 ID 선택
            return this.leaderId != null; // 리더가 존재하는지 여부 반환
        } catch (Exception e) {
            LOG.error("Failed to refresh leader", e);
            return false; // 실패 반환
        }
    }
}