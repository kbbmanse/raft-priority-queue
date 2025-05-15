package com.example.priorityqueue.server;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.example.priorityqueue.common.Element;
import com.example.priorityqueue.common.PriorityQueueClosure;
import com.example.priorityqueue.common.PriorityQueueRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.example.priorityqueue.common.PriorityQueueRequest.deserialize;

public class PriorityQueueStateMachine extends StateMachineAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueStateMachine.class);

    // 우선순위 큐 구현을 위한 힙 자료구조
    private final PriorityQueue<Element> queue = new PriorityQueue<>((e1, e2) ->
            Integer.compare(e2.getPriority(), e1.getPriority()));

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // 로그 적용 처리
    @Override
    @SuppressWarnings("unchecked")
    public void onApply(Iterator iter) {
        lock.writeLock().lock();
        try {
            while (iter.hasNext()) {
                PriorityQueueRequest op = deserialize(iter.getData().array());
                try {
                    switch (op.getType()) {
                        case ENQUEUE:
                            queue.add(op.getElement());
                            PriorityQueueClosure<Boolean> enqueueClosure = null;
                            if (iter.done() != null) {
                                enqueueClosure = (PriorityQueueClosure<Boolean>) iter.done();
                            }
                            if (enqueueClosure != null) {
                                enqueueClosure.setResponse(true);
                                enqueueClosure.run(Status.OK());
                            }
                            LOG.info("Enqueued element: {}", op.getElement());
                            break;
                        case DEQUEUE:
                            PriorityQueueClosure<Element> dequeueClosure = null;
                            if (iter.done() != null) {
                                dequeueClosure = (PriorityQueueClosure<Element>) iter.done();
                            }
                            if (queue.isEmpty()) {
                                if (dequeueClosure != null) {
                                    dequeueClosure.setResponse(null);
                                    dequeueClosure.run(Status.OK());
                                }
                                LOG.info("Dequeue operation on empty queue");
                            } else {
                                Element element = queue.poll();
                                if (dequeueClosure != null) {
                                    dequeueClosure.setResponse(element);
                                    dequeueClosure.run(Status.OK());
                                }
                                LOG.info("Dequeued element: {}", element);
                            }
                            break;
                        case PEEK:
                            PriorityQueueClosure<Element> peekClosure = null;
                            if (iter.done() != null) {
                                peekClosure = (PriorityQueueClosure<Element>) iter.done();
                            }
                            if (queue.isEmpty()) {
                                if (peekClosure != null) {
                                    peekClosure.setResponse(null);
                                    peekClosure.run(Status.OK());
                                }
                                LOG.info("Peek operation on empty queue");
                            } else {
                                Element element = queue.peek();
                                if (peekClosure != null) {
                                    peekClosure.setResponse(element);
                                    peekClosure.run(Status.OK());
                                }
                                LOG.info("Peeked element: {}", element);
                            }
                            break;
                    }
                } catch (Exception e) {
                    LOG.error("Error in processing request", e);
                } finally {
                    iter.next();
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 스냅샷 생성
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        lock.readLock().lock();
        try {
            String path = writer.getPath() + File.separator + "queue_snapshot";
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) {
                oos.writeObject(new ArrayList<>(queue));
                oos.flush();
                writer.addFile("queue_snapshot");
                done.run(Status.OK());
            } catch (IOException e) {
                LOG.error("Failed to save snapshot", e);
                done.run(new Status(RaftError.EIO, "Failed to save snapshot: %s", e.getMessage()));
            }
        } finally {
            lock.readLock().unlock();
        }
    }

    // 스냅샷 로드
    @Override
    @SuppressWarnings("unchecked")
    public boolean onSnapshotLoad(SnapshotReader reader) {
        lock.writeLock().lock();
        try {
            String path = reader.getPath() + File.separator + "queue_snapshot";
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) {
                ArrayList<Element> elements = (ArrayList<Element>) ois.readObject();
                queue.clear();
                queue.addAll(elements);
                LOG.info("Loaded {} elements from snapshot", elements.size());
                return true;
            } catch (IOException | ClassNotFoundException e) {
                LOG.error("Failed to load snapshot", e);
                return false;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Element peek() {
        lock.readLock().lock();
        try {
            return queue.peek();
        } finally {
            lock.readLock().unlock();
        }
    }

}
