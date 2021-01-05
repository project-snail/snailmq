package com.snail;

import com.snail.commit.CommitLog;
import com.snail.commit.impl.SyncFlushDiskHandler;
import com.snail.config.MessageStoreConfig;
import com.snail.consumer.MqService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.RebalanceRequest;
import com.snail.consumer.rebalance.RebalanceResult;
import com.snail.consumer.rebalance.RebalanceService;
import com.snail.message.Message;
import com.snail.message.MessageRes;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.FetchTopicGroupOffsetCommandData;
import com.snail.remoting.command.data.PushMessageCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.store.ByteBufferStoreItem;
import com.snail.store.CommitStore;
import com.snail.util.StoreItemUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail
 * @Description:
 * @date: 2020/12/13
 */
@ActiveProfiles("csz")
@SpringBootTest(classes = ServerApplication.class)
public class TestApplication {

    @Test
    void testCommitLog() throws IOException {

        CommitLog commitLog = new CommitLog(
            0L,
            new File("store/commit_log"),
            1024 * 1024 * 10,
            true,
            new SyncFlushDiskHandler()
        );

        Message message = new Message();
        message.setTopic("testTopic");
        message.setKey("12312");
        message.setFlag(0);
        message.setBody(
            ByteBuffer.wrap("{\"name\": \"张三\"}".getBytes(StandardCharsets.UTF_8))
        );

        commitLog.addMessage(message);

        commitLog.shutdown();

    }

    @Test
    void testBuffer() throws IOException {

        MappedByteBuffer rw = new RandomAccessFile("store/commit_log", "rw").getChannel()
            .map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 1024 * 10);

        ByteBuffer byteBuffer = rw.slice();

        System.out.println(byteBuffer.getInt());

        System.out.println(byteBuffer.getInt());

        System.out.println(byteBuffer.getLong());

        ByteBufferStoreItem deserialize = StoreItemUtil.deserializeWithMovePost(
            byteBuffer,
            ByteBufferStoreItem::deserialize
        );

        ByteBufferStoreItem deserialize2 = StoreItemUtil.deserializeWithMovePost(
            byteBuffer,
            ByteBufferStoreItem::deserialize
        );

        Message message1 = Message.deserialize(deserialize.body());
        Message message2 = Message.deserialize(deserialize2.body());

        System.out.println(StandardCharsets.UTF_8.decode(message1.getBody()));
        System.out.println(StandardCharsets.UTF_8.decode(message2.getBody()));

    }

    @Autowired
    private CommitStore commitStore;

    @Test
    void testSetup() {

        Message message = new Message();
        message.setTopic("testTopic");
        message.setKey("12312");
        message.setFlag(0);
        message.setBody(
            ByteBuffer.wrap("{\"name\": \"张三\"}".getBytes(StandardCharsets.UTF_8))
        );

        commitStore.addMessage(message);

    }

    @Test
    void testGetMessage() {
        MessageRes message = commitStore.getMessage("testTopic", 3, 0L);
        System.out.println(message);
    }

    @Test
    void testFormat() {
        System.out.println(String.format("%020d", 10));
    }

    @Autowired
    private MessageStoreConfig messageStoreConfig;

    @Test
    void testRoll() {

        HashMap<String, Map<Integer, AtomicLong>> map = new HashMap<>();

        StopWatch stopWatch = new StopWatch();

        stopWatch.start("增加并获取消息");

        ByteBuffer wrap = ByteBuffer.wrap("{\"name\": \"张三\"}".getBytes(StandardCharsets.UTF_8));
        for (int i = 0; i < 10000; i++) {
//            System.out.println(i);
            int i1 = i % 2;
            String topic = "testTopic" + i1;
            String key = "12312" + ((i * 13) % 7);
            int queueId = (key.hashCode()) % messageStoreConfig.getQueueSize();
            Message message = new Message();
            message.setTopic(topic);
            message.setKey(key);
            message.setFlag(i1);
            message.setBody(
                wrap.slice()
            );

            commitStore.addMessage(message);

            Map<Integer, AtomicLong> topicMap = map.computeIfAbsent(topic, k -> new HashMap<>());
            AtomicLong index = topicMap.computeIfAbsent(queueId, k -> new AtomicLong());

//            System.out.println("存---------------------");

            MessageRes message1 = commitStore.getMessage(topic, queueId, index.getAndIncrement());

            System.out.println(message1.toString());

//            System.out.println("取---------------------");

        }

        stopWatch.stop();

        System.out.println(stopWatch.prettyPrint());

    }

    @Test
    void testGetOldMessage() {

        MessageRes testTopic1 = commitStore.getMessage("testTopic1", 1, 3L);

        System.out.println(testTopic1.toString());

    }

    @Autowired
    private MqService mqService;

    @Test
    void testConsumerService() {
        String topic = "11";
        String key = "122";
        int queueId = key.hashCode() % messageStoreConfig.getQueueSize();

        Message message = new Message();
        message.setTopic(topic);
        message.setKey(key);
        message.setFlag(1);
        ByteBuffer wrap = ByteBuffer.wrap("{\"name\": \"张三\"}".getBytes(StandardCharsets.UTF_8));
        message.setBody(
            wrap.slice()
        );

        mqService.addMessage(message);

        MessageRes getMeg = mqService.getMessage(
            topic,
            queueId,
            0L
        );

        System.out.println(getMeg);

        TopicGroupOffset groupOffset = new TopicGroupOffset(topic, "testGroup", queueId, 0L);

//        List<TopicGroupOffset> topicGroupOffsetList = mqService.getOffset(
//            Collections.singletonList(groupOffset)
//        );
//
//        mqService.updateOffset(groupOffset);
//
//        topicGroupOffsetList = mqService.getOffset(
//            Collections.singletonList(groupOffset)
//        );
//
//        System.out.println(topicGroupOffsetList);

    }

    @Test
    void testMinOffset() {

        TopicGroupOffset groupOffset = new TopicGroupOffset("testTopic1", "testTopic1", 2, 0L);

        List<TopicGroupOffset> queueMinOffset = this.mqService.getQueueMinOffset(
            Collections.singletonList(groupOffset)
        );

        System.out.println(queueMinOffset);

    }

    @Test
    void testConsumer() {

        String topic = "testConsumer";
        String key = "1221";
        int queueId = key.hashCode() % messageStoreConfig.getQueueSize();

        for (int i = 0; i < 1003; i++) {

            Message message = new Message();
            message.setTopic(topic);
            message.setKey(key);
            message.setFlag(i);
            message.setBody(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)));

            this.mqService.addMessage(message);
        }

        long nextMsgOffset;
//        查找模板
        TopicGroupOffset findOffset = new TopicGroupOffset(topic, "testTopic1", queueId, -1L);
        List<TopicGroupOffset> topicGroupOffsetList = Collections.singletonList(findOffset);

//        查找历史消费记录
        List<TopicGroupConsumerOffset> queueMinOffset = this.mqService.getOffset(topicGroupOffsetList);
//
//        TopicGroupOffset topicGroupOffset = queueMinOffset.get(0);
//
////        如果没消费过
//        if (topicGroupOffset.getOffset() < 0) {
////            获取当前queue最小偏移的消息
//            topicGroupOffset = this.commitStore.getQueueMinOffset(topicGroupOffsetList).get(0);
//            nextMsgOffset = topicGroupOffset.getOffset();
//        } else {
////            获取当前消费记录的下一个消息
//            nextMsgOffset = this.mqService.getNextMsgOffset(Collections.singletonList(topicGroupOffset)).get(0).getOffset();
//        }
//
////        统计消息数
//        AtomicLong count = new AtomicLong();
//
//        while (nextMsgOffset != -1) {
//
////            获取消息
//            MessageRes messageRes = mqService.getMessage(
//                findOffset.getTopic(),
//                findOffset.getQueueId(),
//                nextMsgOffset
//            );
//
//            count.incrementAndGet();
//
//            System.out.println(messageRes.getNextMsgOffset());
//
////            更新消费偏移量
//            topicGroupOffset.setOffset(nextMsgOffset);
//            mqService.updateOffset(topicGroupOffset);
//
//            if (messageRes.getNextMsgOffset() == -1) {
//                System.out.println("消费结束");
//                System.out.println(count.get());
//                break;
//            }
//
////            下一个消息的偏移量
//            nextMsgOffset = messageRes.getNextMsgOffset();
//
//        }


    }

    @Test
    void testFetchCommand() {

        TopicGroupOffset findOffset = new TopicGroupOffset("testTopic1", "testGroup1", 3, -1L);
        List<TopicGroupOffset> topicGroupOffsetList = Collections.singletonList(findOffset);

        FetchTopicGroupOffsetCommandData fetchTopicGroupOffsetCommandData = new FetchTopicGroupOffsetCommandData(
            topicGroupOffsetList);

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.FETCH_TOPIC_GROUP_OFFSET,
            fetchTopicGroupOffsetCommandData
        );

        ByteBuffer serialize = remotingCommand.serialize();

        ByteBuffer slice = serialize.slice();

        RemotingCommand remotingCommand1 = RemotingCommand.deserialize(slice);

        ByteBuffer dataByteBuffer = remotingCommand1.getDataByteBuffer();

        FetchTopicGroupOffsetCommandData deserialize = FetchTopicGroupOffsetCommandData.deserialize(dataByteBuffer);

        System.out.println(deserialize);

    }

    @Test
    void testPushCommand() {

        Message message = new Message();
        message.setTopic("topic");
        message.setKey("key");
        message.setFlag(0);
        message.setBody(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)));

        PushMessageCommandData pushMessageCommandData = new PushMessageCommandData(message);

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.PUSH_MESSAGE,
            pushMessageCommandData
        );

        ByteBuffer serialize = remotingCommand.serialize();

        ByteBuffer slice = serialize.slice();

        RemotingCommand remotingCommand1 = RemotingCommand.deserialize(slice);

        ByteBuffer dataByteBuffer = remotingCommand1.getDataByteBuffer();

        PushMessageCommandData deserialize = PushMessageCommandData.deserialize(dataByteBuffer);

        System.out.println(deserialize);

        Message deserializeMessage = deserialize.getMessage();

        byte[] bytes = new byte[deserializeMessage.getBody().remaining()];

        deserializeMessage.getBody().get(bytes);

        System.out.println(new String(bytes, StandardCharsets.UTF_8));

    }

    /**
     * 再均衡
     * <p>
     * 1 -> [0,1,2,3,4,5]
     * 2 -> [0,1,2] 同时 1 -> [3,4,5]
     * 2离开 需要通知 1 -> [0,1,2,3,4,5]
     * <p>
     * 均衡版本号递增
     * 如果是老的版本号 则直接不接受
     * 让客户端重新获取queue
     */

    @Test
    void testServer() {
        new Scanner(System.in).nextInt();
    }

    @Autowired
    private RebalanceService rebalanceService;

    @Test
    void testRebalance() {

        RebalanceRequest rebalanceRequest = new RebalanceRequest();

        rebalanceRequest.setTopic("testTopic");
        rebalanceRequest.setGroup("testGroup");

        String userOne = "cid1";

        rebalanceService.addCid(userOne, Collections.singletonList(rebalanceRequest));

        List<RebalanceResult> rebalanceResult = rebalanceService.getRebalanceResult(
            userOne,
            Collections.singletonList(rebalanceRequest)
        );

        System.out.println(rebalanceResult);

        String userTwo = "cid2";

        rebalanceService.addCid(userTwo, Collections.singletonList(rebalanceRequest));

        List<RebalanceResult> rebalanceResult2 = rebalanceService.getRebalanceResult(
            userTwo,
            Collections.singletonList(rebalanceRequest)
        );

        System.out.println(rebalanceResult2);

    }
}
