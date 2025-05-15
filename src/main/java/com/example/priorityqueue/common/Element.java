package com.example.priorityqueue.common;

import java.io.Serializable;

public class Element implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String value;
    private final int priority;

    public Element(String value, int priority) {
        this.value = value;
        this.priority = priority;
    }

    public String getValue() {
        return value;
    }

    public int getPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "Element{value='" + value + "', priority=" + priority + '}';
    }
}
