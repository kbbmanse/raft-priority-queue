package com.example.priorityqueue;

import com.example.priorityqueue.client.PriorityQueueClient;
import com.example.priorityqueue.common.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class ClientMain {
    private static final Logger LOG = LoggerFactory.getLogger(ClientMain.class);

    public static void main(String[] args) {
        try {
            if (args.length < 2) {
                System.out.println("Usage: <program> <group_id> <peer_addresses>");
                System.out.println("Example: java -jar priority-queue-client.jar priority_queue_group 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
                System.exit(1);
            }

            String groupId = args[0];
            String peerAddresses = args[1];

            // 클라이언트 인스턴스 생성
            PriorityQueueClient client = new PriorityQueueClient(groupId);
            client.addPeers(peerAddresses);

            // 명령어 처리를 위한 스캐너
            Scanner scanner = new Scanner(System.in);
            System.out.println("Priority Queue Client");
            System.out.println("Available commands:");
            System.out.println("  enqueue <value> <priority> - Add an element to the queue");
            System.out.println("  eq <value> <priority> - Add an element to the queue");
            System.out.println("  dequeue - Remove and return the highest priority element");
            System.out.println("  dq - Remove and return the highest priority element");
            System.out.println("  peek - Return the highest priority element without removing");
            System.out.println("  pk - Return the highest priority element without removing");
            System.out.println("  quit - Exit the client");

            while (true) {
                System.out.print("> ");
                String line = scanner.nextLine().trim();

                if (line.isEmpty()) {
                    continue;
                }

                String[] parts = line.split("\\s+");
                String command = parts[0].toLowerCase();

                try {
                    switch (command) {
                        case "eq":
                        case "enqueue":
                            if (parts.length < 3) {
                                LOG.info("Usage: enqueue <value> <priority>");
                                continue;
                            }
                            String value = parts[1];
                            int priority = Integer.parseInt(parts[2]);
                            boolean result = client.enqueue(value, priority);
                            LOG.info("Enqueue result: {}", result);
                            break;

                        case "dq":
                        case "dequeue":
                            Element dequeued = client.dequeue();
                            if (dequeued != null) {
                                LOG.info("Dequeued: {}", dequeued);
                            } else {
                                LOG.info("Queue is empty or operation failed");
                            }
                            break;

                        case "pk":
                        case "peek":
                            Element peeked = client.peek();
                            if (peeked != null) {
                                LOG.info("Peeked: {}", peeked);
                            } else {
                                LOG.info("Queue is empty or operation failed");
                            }
                            break;

                        case "quit":
                        case "exit":
                            client.shutdown();
                            LOG.info("Client shutdown");
                            return;

                        default:
                            LOG.warn("Unknown command: {}", command);
                            break;
                    }
                } catch (Exception e) {
                    LOG.error("Error executing command: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.error("Error running client", e);
        }
    }
}
