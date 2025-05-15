package com.example.priorityqueue.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    // 직렬화 유틸리티
    public static byte[] serialize(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            return bos.toByteArray();
        } catch (IOException e) {
            LOG.error("Failed to serialize object", e);
            throw new RuntimeException("Failed to serialize object", e);
        }
    }

    public static Object deserialize(byte[] data) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            LOG.error("Failed to deserialize object", e);
            throw new RuntimeException("Failed to deserialize object", e);
        }
    }
}
