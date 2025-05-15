package com.example.priorityqueue.common;

import java.io.Serializable;

public class PriorityQueueResponse<T> implements Serializable {
    private static final long serialVersionUID = 1L;
    private boolean success;
    private T result;
    private String message;

    public PriorityQueueResponse() {
    }

    public PriorityQueueResponse(boolean success, T result, String message) {
        this.success = success;
        this.result = result;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public T getResult() {
        return result;
    }

    public String getMessage() {
        return message;
    }
}
