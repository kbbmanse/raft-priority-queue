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

public class PriorityQueueRequestProcessor implements RpcProcessor<BytesValue> { // RpcProcessor 구현
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueRequestProcessor.class);
    private final Node node;

    public PriorityQueueRequestProcessor(Node node) {
        this.node = node;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, BytesValue request) {
        LOG.info("Received jRaft priority queue request.");
        try {
            PriorityQueueRequest op = deserialize(request.getValue().toByteArray());
            applyToRaft(op, rpcCtx);
        } catch (Exception e) {
            LOG.error("Failed to process request", e);
            sendErrorResponse(rpcCtx, "Failed to process request: " + e.getMessage());
        }
    }

    private PriorityQueueClosure<?> createClosure(PriorityQueueRequest op) {
        switch (op.getType()) {
            case ENQUEUE:
                return new PriorityQueueClosure<Boolean>();
            case DEQUEUE:
            case PEEK:
                return new PriorityQueueClosure<Element>();
            default:
                throw new IllegalArgumentException("Unknown operation type: " + op.getType());
        }
    }

    private void applyToRaft(PriorityQueueRequest op, RpcContext rpcCtx) {
        Task task = new Task();
        task.setData(ByteBuffer.wrap(serialize(op)));
        final PriorityQueueClosure<?> closure = createClosure(op);
        task.setDone(closure);
        node.apply(task);
        closure.getFuture().thenAccept(result -> {
            PriorityQueueResponse<?> response;
            if (op.getType() == ENQUEUE) {
                response = new PriorityQueueResponse<Boolean>(true, (Boolean) result, "");
            } else if (op.getType() == DEQUEUE) {
                response = new PriorityQueueResponse<Element>(true, (Element) result, "");
            } else {// PEEK
                response = new PriorityQueueResponse<Element>(true, (Element) result, "");
            }
            sendSuccessResponse(rpcCtx, BytesValue.newBuilder().setValue(ByteString.copyFrom(serialize(response))).build());
        }).exceptionally(ex -> {
            LOG.error("Failed to apply task to raft", ex);
            sendErrorResponse(rpcCtx, "Failed to apply task to raft: " + ex.getMessage());
            return null;
        });
    }

    private void sendSuccessResponse(RpcContext rpcCtx, BytesValue response) {
        rpcCtx.sendResponse(response);
    }

    private void sendErrorResponse(RpcContext rpcCtx, String errorMessage) {
        BytesValue response = BytesValue.newBuilder()
                .setValue(ByteString.copyFrom(serialize(new PriorityQueueResponse<String>(false, null, errorMessage))))
                .build();
        rpcCtx.sendResponse(response);
    }

    @Override
    public String interest() {
        return BytesValue.class.getName();
    }


}