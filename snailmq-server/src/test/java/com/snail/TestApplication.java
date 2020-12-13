package com.snail;

import com.snail.commit.CommitLog;
import com.snail.message.Message;
import com.snail.store.ByteBufferStoreItem;
import com.snail.util.StoreItemUtil;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail
 * @Description:
 * @date: 2020/12/13
 */
@SpringBootTest(classes = ServerApplication.class)
public class TestApplication {

    @Test
    void testCommitLog() throws IOException {

        CommitLog commitLog = new CommitLog(
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
}
