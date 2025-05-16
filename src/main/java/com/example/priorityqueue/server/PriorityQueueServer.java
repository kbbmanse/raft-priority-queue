package com.example.priorityqueue.server;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.example.priorityqueue.common.Element;
import com.example.priorityqueue.common.PriorityQueueRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class PriorityQueueServer {
    private static final Logger LOG = LoggerFactory.getLogger(PriorityQueueServer.class);

    private final RaftGroupService raftGroupService; // Raft 그룹 서비스를 관리하는 객체
    private Node node; // Raft 노드
    private final PriorityQueueStateMachine stateMachine; // 상태 머신 (우선순위 큐)
    private final PeerId serverId; // 서버 ID
    private final RpcServer rpcServer; // RPC 서버
    private final AtomicBoolean started = new AtomicBoolean(false); // 서버 시작 여부를 나타내는 AtomicBoolean

    /**
     * PriorityQueueServer의 생성자.
     *
     * @param dataPath  데이터를 저장할 경로 (로그, 메타데이터, 스냅샷)
     * @param groupId   Raft 그룹 ID
     * @param serverId  서버 ID
     * @param peerIds   Raft 그룹에 참여하는 다른 피어들의 ID 목록
     * @throws IOException 입출력 예외 발생 시
     */
    public PriorityQueueServer(String dataPath, String groupId, PeerId serverId, List<PeerId> peerIds) throws IOException {
        this.serverId = serverId; // 서버 ID 설정

        // RPC 서버 초기화
        this.rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint()); // 서버 엔드포인트를 기반으로 RPC 서버 생성

        // Raft 옵션 설정
        RaftOptions raftOptions = new RaftOptions(); // Raft 옵션 객체 생성 (필요한 경우 추가적인 옵션 설정 가능)

        // 상태 머신 생성
        this.stateMachine = new PriorityQueueStateMachine(); // 우선순위 큐 상태 머신 생성

        // 노드 옵션 설정
        NodeOptions nodeOptions = new NodeOptions(); // Raft 노드 옵션 객체 생성
        nodeOptions.setFsm(this.stateMachine); // 상태 머신 설정
        nodeOptions.setLogUri(dataPath + File.separator + "log"); // 로그 저장 경로 설정
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta"); // Raft 메타데이터 저장 경로 설정
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot"); // 스냅샷 저장 경로 설정
        nodeOptions.setInitialConf(new Configuration(peerIds)); // 초기 Raft 그룹 구성 설정 (피어 목록)
        nodeOptions.setSnapshotIntervalSecs(30); // 스냅샷 생성 간격 (초)

        // Raft 그룹 서비스 및 노드 초기화
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer); // Raft 그룹 서비스 생성
    }

    /**
     * 서버를 시작합니다.
     *
     * @return 시작 성공 여부
     */
    public boolean start() {
        if (!started.compareAndSet(false, true)) { // AtomicBoolean을 사용하여 시작 여부 확인 및 설정
            return false; // 이미 시작되었으면 false 반환
        }

        // 노드 시작
        this.node = this.raftGroupService.start(); // Raft 그룹 서비스 시작 (Raft 노드 시작)
        if (this.node != null) { // 노드 시작 성공 시
            LOG.info("Priority queue server started on {}", serverId);
            // client rpc 요청을 처리하는 핸들러 등록
            rpcServer.registerProcessor(new PriorityQueueRequestProcessor(node)); // RPC 요청을 처리할 프로세서 등록
            return true; // 시작 성공 반환
        }
        return false; // 시작 실패 반환
    }

    /**
     * 서버를 종료합니다.
     */
    public void shutdown() {
        if (!started.compareAndSet(true, false)) { // AtomicBoolean을 사용하여 시작 여부 확인 및 설정
            return; // 이미 종료되었으면 반환
        }

        if (this.raftGroupService != null) { // Raft 그룹 서비스가 존재하면
            this.raftGroupService.shutdown(); // Raft 그룹 서비스 종료
            LOG.info("Raft group service shutdown");
        }

        if (this.rpcServer != null) { // RPC 서버가 존재하면
            this.rpcServer.shutdown(); // RPC 서버 종료
            LOG.info("RPC server shutdown");
        }

        LOG.info("Priority queue server shutdown");
    }

    /**
     * 우선순위 큐를 조회합니다 (리더에서 직접 접근).
     *
     * @return 큐의 head에 있는 Element, 리더가 아니면 null 반환
     */
    public Element peek() {
        if (!isLeader()) { // 리더 여부 확인
            LOG.warn("Not leader, cannot provide local service");
            return null; // 리더가 아니면 null 반환
        }
        return this.stateMachine.peek(); // 상태 머신에서 peek() 호출
    }

    /**
     * 현재 노드가 리더인지 확인합니다.
     *
     * @return 리더 여부
     */
    public boolean isLeader() {
        return this.node != null && this.node.isLeader(); // 노드가 존재하고 리더인지 확인
    }

}