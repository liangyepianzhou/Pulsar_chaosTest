package org.example;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.common.policies.data.TransactionBufferStats;

/**
 * 1. snapshot segment size.
 * 2. snapshot index.
 * 3. unsealed aborted transactions.
 *
 * Snapshot segment and unsealed aborted transactions should be matched to maxReadPosition.
 *
 * maxReadPosition 更新到某个位置之后，snapshot segment也会更新到对应的数量。
 * snapshot index 不一定需要和segment对应的上 所以不需要单独测index
 * 给定一个maxReadPosition，那么一定读取不到aborted transaction message
 * //TODO：增加 Admin API： 应该获取的核心数据是： maxReadPosition，segment size，unseal aborted transaction IDs number
 *
 * 测试的逻辑是：
 * <p>
 *     线程A不断的使用transaction发送单条消息，并且abort这些transaction。
 *   记录每个maxReadPosition对应的segment size 和unseal aborts的数量。
 *   每发完一个segment的transaction之后，发一个普通消息。
 *     线程B使用admin tool获取snapshot 的数据，判断maxReadPosition对应的 snapshot segment size和
 *   unseal aborted transaction IDs 的数量 是否对的上
 *     线程C创建reader 去读取这个topic的消息，保证读取不到aborted transaction的消息。
 *   每个小时重新创建一次Reader 去重新读取消息。
 *    线程D每隔30分钟unload 一次 test topic.
 * </p>
 */

@Slf4j
public class TransactionMultipleSnapshotTest {

    public static PulsarClient pulsarClient;
    public static String testTopic = "persistent://public/default/testMultipleSnapshotsTest" +
            RandomStringUtils.random(5);
    public static String testSubName = "subName";

    public static Set<TxnID> abortedTxnIDs = new HashSet<>();
    public static Map<Position, Long> maxReadPositionMap = new ConcurrentHashMap<>();
    public static AtomicLong abortedTxnCounter = new AtomicLong(0);
    public static Set<MessageId> receiveMessageIds = new HashSet<>();
    /**
     * Make the number of transaction IDs in snapshot segment is 500.
     */
    public static int segmentCapital = 500;

    static {
        try {
            pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650")
                    .enableTransaction(true).build();
            pulsarClient.newConsumer().topic(testTopic).subscriptionName("init_topic_consumer").subscribe();
        } catch (PulsarClientException e) {
            log.error("Failed to connect to pulsar://localhost:6650", e);
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(4);
        executor.submit(() -> {
            try {
                sendThreadA();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.submit(() -> {
            try {
                verifyMultipleSnapshotThreadB();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.submit(() -> {
            try {
                verifyMessagesThreadC();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        executor.submit(() -> {
            try {
                unloadTopicThreadD();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }


    private static void sendThreadA() throws Exception {
        Producer<byte[]> producer = pulsarClient
                .newProducer()
                .topic(testTopic)
                .enableBatching(false)
                .create();
        MessageIdImpl messageID;

        while (true) {
            Transaction ongoingTxn = pulsarClient.newTransaction()
                    .withTransactionTimeout(1, TimeUnit.HOURS)
                    .build().get();
            //This messageID can be used to calculative the maxReadPosition.
            messageID = (MessageIdImpl) producer.newMessage(ongoingTxn).value("message".getBytes()).send();
            for (int i = 0; i < segmentCapital / 3; i++) {
                Transaction transaction = pulsarClient.newTransaction()
                        .withTransactionTimeout(1, TimeUnit.MINUTES)
                        .build().get();
                producer.newMessage(transaction).value("message".getBytes()).send();
                transaction.abort().get();
                abortedTxnCounter.incrementAndGet();
            }
            Position maxReadPosition = new Position(messageID.getLedgerId(), messageID.getEntryId() - 1);
            //We can use the total size of aborted transaction to calculative the segment size and unsealed aborts.
            maxReadPositionMap.put(maxReadPosition, abortedTxnCounter.get());
            log.info("record max read position [{}], aborts counter [{}]", maxReadPosition.toString(),
                    abortedTxnCounter.get());
            //Send 2 messages that can be received by consumer to make the maxReadPosition move.
            receiveMessageIds.add(messageID);
            ongoingTxn.commit().get();
            MessageId messageId = producer.newMessage().value("message".getBytes()).send();
            receiveMessageIds.add(messageId);
            log.info("send message [{}], commit txn message [{}]", messageID.toString(), messageId.toString());
        }
    }

    private static void verifyMultipleSnapshotThreadB() throws Exception {
        PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
        TransactionBufferStats stats = admin.transactions().getTransactionBufferStats(testTopic);
        long[] arr = Arrays.stream(stats.maxReadPosition.split(":")).mapToLong(Long::getLong).toArray();
        //获取对应aborted transaction IDs 的数量
        long abortedTxnSize = maxReadPositionMap.get(new Position(arr[0], arr[1]));
        long segmentSize = abortedTxnSize / segmentCapital;
        long unsealedAborts = abortedTxnSize % segmentCapital;
        //Because the processor record aborted transaction ID first and record max read position secondly,
        //the unsealed aborted transaction IDs should >= the real unsealedAborts.
        if (segmentSize != stats.segmentSize || unsealedAborts <= stats.unsealedAbortTxnIDs) {
            log.error("Expect segmentSize [{}] but found [{}]", segmentSize, stats.segmentSize);
            log.error("Expect unsealedAborts [{}] but found [{}]", unsealedAborts, stats.unsealedAbortTxnIDs);
            System.exit(0);
        }
    }

    private static void verifyMessagesThreadC() throws Exception {
        long gap = TimeUnit.HOURS.toMillis(1);
        //Create a new reader to verify the messages in the topic every hour.
        while (true) {
            Reader<byte[]> reader = pulsarClient
                    .newReader()
                    .topic(testTopic)
                    .subscriptionName(testSubName)
                    .startMessageId(MessageId.earliest)
                    .create();
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < gap) {
                Message<byte[]> message = reader.readNext();
                if (message != null && !receiveMessageIds.contains(message.getMessageId())) {
                    log.error("Receive a aborted message [{}]", message.getMessageId());
                    System.exit(1);
                }
            }
        }
    }

    public static void unloadTopicThreadD() throws Exception {
        //Unload the origin topic every 30 minutes.
        while (true) {
            PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl("http://localhost:8080").build();
            admin.topics().unload(testTopic);
            log.info("Unload topic");
            Thread.sleep(TimeUnit.MINUTES.toMillis(30));
        }
    }

    public static class Position {
        public long ledgerID;
        public long entryID;

        public Position(long ledgerID, long entryID) {
            this.ledgerID = ledgerID;
            this.entryID = entryID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Position position = (Position) o;
            return ledgerID == position.ledgerID && entryID == position.entryID;
        }

        @Override
        public int hashCode() {
            return Objects.hash(ledgerID, entryID);
        }

        @Override
        public String toString() {
            return "Position{" +
                    "ledgerID=" + ledgerID +
                    ", entryID=" + entryID +
                    '}';
        }
    }

}
