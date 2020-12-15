package com.snail;

import com.snail.commit.CommitLog;
import com.snail.config.MessageStoreConfig;
import com.snail.message.Message;
import com.snail.store.ByteBufferStoreItem;
import com.snail.store.CommitStore;
import com.snail.util.StoreItemUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.util.StopWatch;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
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
            true
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
        Message message = commitStore.getMessage("testTopic", 3, 0L);
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
        for (int i = 0; i < 100000; i++) {
//            System.out.println(i);
            int i1 = i % 5;
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

            message = commitStore.getMessage(topic, queueId, index.getAndIncrement());

//            System.out.println(message.toString());

//            System.out.println("取---------------------");

        }

        stopWatch.stop();

        System.out.println(stopWatch.prettyPrint());

    }

    @Test
    void testGetOldMessage() {

        Message testTopic1 = commitStore.getMessage("testTopic1", 1, 3L);

        System.out.println(testTopic1.toString());

    }
}
