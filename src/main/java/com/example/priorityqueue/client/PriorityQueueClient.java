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

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.example.priorityqueue.common.Utils.deserialize;
import static com.example.priorityqueue.common.Utils.serialize;

public class PriorityQueueClient {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueClient.class);

    private final CliClientServiceImpl cliClientService;
    private final String groupId;
    private PeerId leaderId;
    private final int rpcTimeoutMs = 5000;

    public PriorityQueueClient(String groupId) {
        this.cliClientService = new CliClientServiceImpl();
        this.cliClientService.init(new CliOptions());
        this.groupId = groupId;
    }

    public void addPeers(String confStr) {
        RouteTable.getInstance().updateConfiguration(groupId, JRaftUtils.getConfiguration(confStr));
        refreshLeader();
    }

    @SuppressWarnings("unchecked")
    private <T> T processOperation(OperationType type, Element element) throws Exception {
        if (!refreshLeader()) {
            throw new IllegalStateException("Leader not available");
        }

        final BytesValue msg = BytesValue.of(ByteString.copyFrom(serialize(new PriorityQueueRequest(type, element))));
        CompletableFuture<T> future = new CompletableFuture<>();
        RpcResponseClosureAdapter<BytesValue> closureAdapter = new RpcResponseClosureAdapter<>() {
            @Override
            public void run(Status status) {
                if (status.isOk()) {
                    BytesValue response = getResponse();
                    byte[] responseData = response.getValue().toByteArray();
                    PriorityQueueResponse<?> queueResponse = (PriorityQueueResponse<?>) deserialize(responseData);
                    future.complete((T) queueResponse.getResult());
                } else {
                    future.completeExceptionally(new Throwable(status.getErrorMsg()));
                }
            }
        };
        cliClientService.invokeWithDone(
                leaderId.getEndpoint(),
                msg,     // Message 타입으로 래핑된 객체
                closureAdapter,
                rpcTimeoutMs
        );
        // closure에서 결과 대기 및 반환
        return future.get(rpcTimeoutMs, TimeUnit.MILLISECONDS);
    }

    @SuppressWarnings("unchecked")
    private <T> T parseResult(byte[] resultData) throws Exception {
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(resultData))) {
            return (T) ois.readObject();
        }
    }

    public boolean enqueue(String value, int priority) throws Exception {
        return processOperation(OperationType.ENQUEUE, new Element(value, priority));
    }

    public Element dequeue() throws Exception {
        return processOperation(OperationType.DEQUEUE, null);
    }

    public Element peek() throws Exception {
        return processOperation(OperationType.PEEK, null);
    }

    public void shutdown() {
        this.cliClientService.shutdown();
    }

    private boolean refreshLeader() {
        try {
            Status status = RouteTable.getInstance()
                    .refreshLeader(this.cliClientService, this.groupId, 1000);
            if (!status.isOk()) {
                LOG.error("Failed to refresh leader: {}", status);
                return false;
            }
            this.leaderId = RouteTable.getInstance().selectLeader(this.groupId);
            return this.leaderId != null;
        } catch (Exception e) {
            LOG.error("Failed to refresh leader", e);
            return false;
        }
    }
}
