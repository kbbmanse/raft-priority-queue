package com.example.priorityqueue.common;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.error.RaftException;

import java.util.concurrent.CompletableFuture;

public class PriorityQueueClosure<T> implements Closure {
    private final CompletableFuture<T> future;
    private T response;

    public PriorityQueueClosure() {
        this.future = new CompletableFuture<>();
    }

    public CompletableFuture<T> getFuture() {
        return future;
    }

    public T getResponse() {
        return response;
    }

    public void setResponse(T response) {
        this.response = response;
    }

    @Override
    public void run(Status status) {
        if (status.isOk()) {
            future.complete(response);
        } else {
            future.completeExceptionally(new RaftException(EnumOutter.ErrorType.ERROR_TYPE_STATE_MACHINE, status));
        }
    }
}
