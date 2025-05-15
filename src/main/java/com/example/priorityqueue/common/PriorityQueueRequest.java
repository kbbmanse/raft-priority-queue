package com.example.priorityqueue.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class PriorityQueueRequest implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueRequest.class);
    private static final long serialVersionUID = 1L;

    private final OperationType type;
    private final Element element;

    public PriorityQueueRequest(OperationType type, Element element) {
        this.type = type;
        this.element = element;
    }

    public OperationType getType() {
        return type;
    }

    public Element getElement() {
        return element;
    }

    public static PriorityQueueRequest deserialize(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (PriorityQueueRequest) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Failed to deserialize operation", e);
            throw new RuntimeException("Failed to deserialize operation", e);
        }
    }
}
