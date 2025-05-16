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

/**
 * Raft 상태 머신을 구현하여 우선순위 큐를 관리합니다.
 * StateMachineAdapter를 상속받아 Raft 프레임워크와 연동됩니다.
 */
public class PriorityQueueStateMachine extends StateMachineAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueStateMachine.class);

    // 우선순위 큐 구현을 위한 힙 자료구조
    private final PriorityQueue<Element> queue = new PriorityQueue<>((e1, e2) ->
            Integer.compare(e2.getPriority(), e1.getPriority())); // 우선순위가 높은 요소가 먼저 나오도록 정렬

    private final ReadWriteLock lock = new ReentrantReadWriteLock(); // 큐 접근을 동기화하기 위한 ReadWriteLock

    /**
     * Raft 로그를 적용하여 상태 머신을 업데이트합니다.
     *
     * @param iter 로그 항목에 대한 반복자
     */
    @Override
    @SuppressWarnings("unchecked")
    public void onApply(Iterator iter) {
        lock.writeLock().lock(); // 쓰기 락 획득
        try {
            while (iter.hasNext()) { // 로그 항목을 순회
                PriorityQueueRequest op = deserialize(iter.getData().array()); // 로그 데이터에서 PriorityQueueRequest 역직렬화
                try {
                    switch (op.getType()) { // 요청 타입에 따라 처리
                        case ENQUEUE: // 삽입 연산
                            queue.add(op.getElement()); // 큐에 요소 삽입
                            PriorityQueueClosure<Boolean> enqueueClosure = null; // 클로저 초기화
                            if (iter.done() != null) { // 클로저가 존재하는 경우
                                enqueueClosure = (PriorityQueueClosure<Boolean>) iter.done(); // 클로저 캐스팅
                            }
                            if (enqueueClosure != null) { // 클로저가 존재하는 경우
                                enqueueClosure.setResponse(true); // 응답 설정
                                enqueueClosure.run(Status.OK()); // 클로저 실행
                            }
                            LOG.info("Enqueued element: {}", op.getElement());
                            break;
                        case DEQUEUE: // 삭제 연산
                            PriorityQueueClosure<Element> dequeueClosure = null; // 클로저 초기화
                            if (iter.done() != null) { // 클로저가 존재하는 경우
                                dequeueClosure = (PriorityQueueClosure<Element>) iter.done(); // 클로저 캐스팅
                            }
                            if (queue.isEmpty()) { // 큐가 비어있는 경우
                                if (dequeueClosure != null) { // 클로저가 존재하는 경우
                                    dequeueClosure.setResponse(null); // 응답 설정 (null)
                                    dequeueClosure.run(Status.OK()); // 클로저 실행
                                }
                                LOG.info("Dequeue operation on empty queue");
                            } else { // 큐가 비어있지 않은 경우
                                Element element = queue.poll(); // 큐에서 요소 삭제 (가장 우선순위가 높은 요소)
                                if (dequeueClosure != null) { // 클로저가 존재하는 경우
                                    dequeueClosure.setResponse(element); // 응답 설정 (삭제된 요소)
                                    dequeueClosure.run(Status.OK()); // 클로저 실행
                                }
                                LOG.info("Dequeued element: {}", element);
                            }
                            break;
                        case PEEK: // 조회 연산
                            PriorityQueueClosure<Element> peekClosure = null; // 클로저 초기화
                            if (iter.done() != null) { // 클로저가 존재하는 경우
                                peekClosure = (PriorityQueueClosure<Element>) iter.done(); // 클로저 캐스팅
                            }
                            if (queue.isEmpty()) { // 큐가 비어있는 경우
                                if (peekClosure != null) { // 클로저가 존재하는 경우
                                    peekClosure.setResponse(null); // 응답 설정 (null)
                                    peekClosure.run(Status.OK()); // 클로저 실행
                                }
                                LOG.info("Peek operation on empty queue");
                            } else { // 큐가 비어있지 않은 경우
                                Element element = queue.peek(); // 큐에서 요소 조회 (가장 우선순위가 높은 요소)
                                if (peekClosure != null) { // 클로저가 존재하는 경우
                                    peekClosure.setResponse(element); // 응답 설정 (조회된 요소)
                                    peekClosure.run(Status.OK()); // 클로저 실행
                                }
                                LOG.info("Peeked element: {}", element);
                            }
                            break;
                    }
                } catch (Exception e) { // 예외 발생 시
                    LOG.error("Error in processing request", e);
                } finally {
                    iter.next(); // 다음 로그 항목으로 이동
                }
            }
        } finally {
            lock.writeLock().unlock(); // 쓰기 락 해제
        }
    }

    /**
     * 상태 머신의 스냅샷을 생성합니다.
     *
     * @param writer 스냅샷을 작성할 SnapshotWriter
     * @param done   스냅샷 저장 완료 후 실행할 Closure
     */
    @Override
    public void onSnapshotSave(SnapshotWriter writer, Closure done) {
        lock.readLock().lock(); // 읽기 락 획득
        try {
            String path = writer.getPath() + File.separator + "queue_snapshot"; // 스냅샷 파일 경로 생성
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path))) { // 객체 출력 스트림 생성
                oos.writeObject(new ArrayList<>(queue)); // 큐의 내용을 ArrayList로 변환하여 직렬화
                oos.flush(); // 버퍼 비우기
                writer.addFile("queue_snapshot"); // 스냅샷 파일 추가
                done.run(Status.OK()); // 완료 클로저 실행
            } catch (IOException e) { // 입출력 예외 발생 시
                LOG.error("Failed to save snapshot", e);
                done.run(new Status(RaftError.EIO, "Failed to save snapshot: %s", e.getMessage())); // 에러 클로저 실행
            }
        } finally {
            lock.readLock().unlock(); // 읽기 락 해제
        }
    }

    /**
     * 상태 머신의 스냅샷을 로드합니다.
     *
     * @param reader 스냅샷을 읽을 SnapshotReader
     * @return 스냅샷 로드 성공 여부
     */
    @Override
    @SuppressWarnings("unchecked")
    public boolean onSnapshotLoad(SnapshotReader reader) {
        lock.writeLock().lock(); // 쓰기 락 획득
        try {
            String path = reader.getPath() + File.separator + "queue_snapshot"; // 스냅샷 파일 경로 생성
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path))) { // 객체 입력 스트림 생성
                ArrayList<Element> elements = (ArrayList<Element>) ois.readObject(); // 스냅샷에서 요소 목록 역직렬화
                queue.clear(); // 큐 초기화
                queue.addAll(elements); // 스냅샷에서 로드된 요소들을 큐에 추가
                LOG.info("Loaded {} elements from snapshot", elements.size());
                return true;
            } catch (IOException | ClassNotFoundException e) { // 입출력 또는 클래스 관련 예외 발생 시
                LOG.error("Failed to load snapshot", e);
                return false;
            }
        } finally {
            lock.writeLock().unlock(); // 쓰기 락 해제
        }
    }

    /**
     * 큐의 head에 있는 element 를 반환 합니다.
     * @return 큐의 head에 있는 element
     */
    public Element peek() {
        lock.readLock().lock(); // 읽기 락 획득
        try {
            return queue.peek(); // 큐의 head에 있는 element 반환
        } finally {
            lock.readLock().unlock(); // 읽기 락 해제
        }
    }

}