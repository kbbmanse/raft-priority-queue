package com.example.priorityqueue.common;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.google.protobuf.ByteString;
import com.google.protobuf.BytesValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static com.example.priorityqueue.common.OperationType.DEQUEUE;
import static com.example.priorityqueue.common.OperationType.ENQUEUE;
import static com.example.priorityqueue.common.PriorityQueueRequest.deserialize;
import static com.example.priorityqueue.common.Utils.serialize;

/**
 *  `PriorityQueueRequest`를 처리하는 `RpcProcessor` 구현체입니다. 클라이언트로부터의 요청을 받아서 Raft 노드에 적용합니다.
 */
public class PriorityQueueRequestProcessor implements RpcProcessor<BytesValue> { // RpcProcessor 구현
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueRequestProcessor.class);
    private final Node node; // Raft 노드

    /**
     * 생성자
     *
     * @param node Raft 노드
     */
    public PriorityQueueRequestProcessor(Node node) {
        this.node = node;
    }

    /**
     * RPC 요청을 처리하는 메서드.
     *
     * @param rpcCtx RPC 컨텍스트
     * @param request 요청 객체 (BytesValue)
     */
    @Override
    public void handleRequest(RpcContext rpcCtx, BytesValue request) {
        LOG.info("Received jRaft priority queue request.");
        try {
            PriorityQueueRequest op = deserialize(request.getValue().toByteArray()); // 요청 역직렬화
            applyToRaft(op, rpcCtx); // Raft에 적용
        } catch (Exception e) {
            LOG.error("Failed to process request", e);
            sendErrorResponse(rpcCtx, "Failed to process request: " + e.getMessage()); // 에러 응답 전송
        }
    }

    /**
     * 요청 타입에 따라 적절한 `PriorityQueueClosure`를 생성합니다.
     *
     * @param op 요청 객체
     * @return 생성된 `PriorityQueueClosure`
     */
    private PriorityQueueClosure<?> createClosure(PriorityQueueRequest op) {
        switch (op.getType()) {
            case ENQUEUE:
                return new PriorityQueueClosure<Boolean>(); // enqueue 요청에 대한 클로저 생성
            case DEQUEUE:
            case PEEK:
                return new PriorityQueueClosure<Element>(); // dequeue/peek 요청에 대한 클로저 생성
            default:
                throw new IllegalArgumentException("Unknown operation type: " + op.getType()); // 알 수 없는 요청 타입 예외 발생
        }
    }

    /**
     * 요청을 Raft에 적용합니다.
     *
     * @param op     요청 객체
     * @param rpcCtx RPC 컨텍스트
     */
    private void applyToRaft(PriorityQueueRequest op, RpcContext rpcCtx) {
        Task task = new Task(); // Raft 태스크 생성
        task.setData(ByteBuffer.wrap(serialize(op))); // 요청 직렬화 후 태스크 데이터 설정
        final PriorityQueueClosure<?> closure = createClosure(op); // 클로저 생성
        task.setDone(closure); // 태스크에 클로저 설정
        node.apply(task); // Raft 노드에 태스크 적용

        // 비동기적으로 결과를 처리합니다.
        closure.getFuture().thenAccept(result -> {
            PriorityQueueResponse<?> response;
            if (op.getType() == ENQUEUE) {
                response = new PriorityQueueResponse<Boolean>(true, (Boolean) result, ""); // enqueue 응답 생성
            } else if (op.getType() == DEQUEUE) {
                response = new PriorityQueueResponse<Element>(true, (Element) result, ""); // dequeue 응답 생성
            } else {// PEEK
                response = new PriorityQueueResponse<Element>(true, (Element) result, ""); // peek 응답 생성
            }
            sendSuccessResponse(rpcCtx, BytesValue.newBuilder().setValue(ByteString.copyFrom(serialize(response))).build()); // 성공 응답 전송
        }).exceptionally(ex -> { // 예외 발생 시
            LOG.error("Failed to apply task to raft", ex);
            sendErrorResponse(rpcCtx, "Failed to apply task to raft: " + ex.getMessage()); // 에러 응답 전송
            return null;
        });
    }

    /**
     * 성공 응답을 전송합니다.
     *
     * @param rpcCtx   RPC 컨텍스트
     * @param response 응답 객체 (BytesValue)
     */
    private void sendSuccessResponse(RpcContext rpcCtx, BytesValue response) {
        rpcCtx.sendResponse(response); // 응답 전송
    }

    /**
     * 에러 응답을 전송합니다.
     *
     * @param rpcCtx       RPC 컨텍스트
     * @param errorMessage 에러 메시지
     */
    private void sendErrorResponse(RpcContext rpcCtx, String errorMessage) {
        BytesValue response = BytesValue.newBuilder()
                .setValue(ByteString.copyFrom(serialize(new PriorityQueueResponse<String>(false, null, errorMessage)))) // 에러 응답 생성
                .build();
        rpcCtx.sendResponse(response); // 응답 전송
    }

    /**
     * 해당 Processor가 처리할 메시지 타입을 반환합니다.
     *
     * @return 메시지 타입 이름
     */
    @Override
    public String interest() {
        return BytesValue.class.getName(); // BytesValue 타입을 처리
    }
}