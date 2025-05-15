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

    private final RaftGroupService raftGroupService;
    private Node node;
    private final PriorityQueueStateMachine stateMachine;
    private final String dataPath;
    private final String groupId;
    private final PeerId serverId;
    private final RpcServer rpcServer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    public PriorityQueueServer(String dataPath, String groupId, PeerId serverId, List<PeerId> peerIds) throws IOException {
        this.dataPath = dataPath;
        this.groupId = groupId;
        this.serverId = serverId;

        // RPC 서버 초기화
        this.rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());

        // Raft 옵션 설정
        RaftOptions raftOptions = new RaftOptions();

        // 상태 머신 생성
        this.stateMachine = new PriorityQueueStateMachine();

        // 노드 옵션 설정
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(this.stateMachine);
        nodeOptions.setLogUri(dataPath + File.separator + "log");
        nodeOptions.setRaftMetaUri(dataPath + File.separator + "raft_meta");
        nodeOptions.setSnapshotUri(dataPath + File.separator + "snapshot");
        nodeOptions.setInitialConf(new Configuration(peerIds));
        nodeOptions.setSnapshotIntervalSecs(30);

        // Raft 그룹 서비스 및 노드 초기화
        this.raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer);
    }

    // 서버 시작
    public boolean start() {
        if (!started.compareAndSet(false, true)) {
            return false;
        }

        // 노드 시작
        this.node = this.raftGroupService.start();
        if (this.node != null) {
            LOG.info("Priority queue server started on {}", serverId);
            // client rpc 요청을 처리하는 핸들러 등록
            rpcServer.registerProcessor(new PriorityQueueRequestProcessor(node));
            return true;
        }
        return false;
    }

    // 서버 종료
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        if (this.raftGroupService != null) {
            this.raftGroupService.shutdown();
            LOG.info("Raft group service shutdown");
        }

        if (this.rpcServer != null) {
            this.rpcServer.shutdown();
            LOG.info("RPC server shutdown");
        }

        LOG.info("Priority queue server shutdown");
    }

    // 우선순위 큐 조회 (리더 직접 접근)
    public Element peek() {
        if (!isLeader()) {
            LOG.warn("Not leader, cannot provide local service");
            return null;
        }
        return this.stateMachine.peek();
    }

    // 리더 여부 확인
    public boolean isLeader() {
        return this.node != null && this.node.isLeader();
    }

}
