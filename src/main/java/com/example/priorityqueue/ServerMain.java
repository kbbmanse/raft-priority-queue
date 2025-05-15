package com.example.priorityqueue;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.PeerId;
import com.example.priorityqueue.server.PriorityQueueServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class ServerMain {
    private static final Logger LOG = LoggerFactory.getLogger(ServerMain.class);

    public static void main(String[] args) {
        try {
            if (args.length < 3) {
                System.out.println("Usage: <program> <data_path> <group_id> <server_id> [peer1,peer2,peer3...]");
                System.out.println("Example: java -jar priority-queue-server.jar /tmp/server1 priority_queue_group 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
                System.exit(1);
            }

            String dataPath = args[0];
            String groupId = args[1];
            String serverId = args[2];
            String peersStr = args.length > 3 ? args[3] : serverId;

            // 경로 준비
            File dir = new File(dataPath);
            if (!dir.exists() && !dir.mkdirs()) {
                throw new RuntimeException("Failed to create directory: " + dataPath);
            }

            // 피어 목록 준비
            List<PeerId> peers = new ArrayList<>();
            for (String peerStr : peersStr.split(",")) {
                peers.add(JRaftUtils.getPeerId((peerStr)));
            }

            // 서버 인스턴스 생성 및 시작
            PriorityQueueServer server = new PriorityQueueServer(dataPath, groupId, JRaftUtils.getPeerId(serverId), peers);
            if (server.start()) {
                LOG.info("Server started at {}", serverId);

                // 명령어 처리를 위한 스캐너
                Scanner scanner = new Scanner(System.in);
                LOG.info("Server is running. Type 'quit' to exit.");

                while (true) {
                    String command = scanner.nextLine();
                    if ("quit".equalsIgnoreCase(command)) {
                        break;
                    } else if ("isleader".equalsIgnoreCase(command)) {
                        LOG.info("Is leader: {}", server.isLeader());
                    } else if ("peek".equalsIgnoreCase(command)) {
                        if (server.isLeader()) {
                            LOG.info("Top element: {}", server.peek());
                        } else {
                            LOG.info("Not leader, cannot peek directly");
                        }
                    } else {
                        LOG.info("Unknown command. Available: quit, isleader, peek");
                    }
                }

                scanner.close();
                server.shutdown();
            } else {
                LOG.error("Failed to start server");
            }
        } catch (Exception e) {
            LOG.error("Error running server", e);
        }
    }
}
