package com.snail.commit;

import com.snail.config.MessageStoreConfig;
import com.snail.mapped.MappedFile;
import com.snail.mapped.SelectMappedBuffer;
import com.snail.message.Message;
import com.snail.message.MessageExt;
import com.snail.store.ByteBufferStoreItem;
import com.snail.util.StoreItemUtil;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.commit
 * @Description:
 * @date: 2020/12/13
 */
@Data
public class CommitLog {

    private Long startOffset;

    //    魔数
    public final static int MESSAGE_MAGIC_CODE = MessageStoreConfig.MESSAGE_MAGIC_CODE;

    //    写指针
    private AtomicInteger writePos;

    //    最后一个添加消息的时间戳
    private AtomicLong latestWriteTimeStamp;

    //    log文件映射对象
    private MappedFile mappedFile;

    //    log文件区域映射对象
    private SelectMappedBuffer selectMappedBuffer;

    //    用于持久化写指针的buffer
    private SelectMappedBuffer writePosSelectMappedBuffer;

    //    这个文件最大的大小
    private int maxFileSize;

    public CommitLog(Long startOffset, File file, int fileSize, boolean autoCreate) throws IOException {
        this.startOffset = startOffset;
        this.mappedFile = new MappedFile(file, fileSize, autoCreate);
        this.maxFileSize = fileSize;
        init();
        this.selectMappedBuffer = mappedFile.select(writePos.get(), fileSize - writePos.get());
        this.writePosSelectMappedBuffer = mappedFile.select(4, 12);
    }

    private void init() {
        /*
        magic 4| 写指针位置 4 |最后一次写时间戳 8
         */
        SelectMappedBuffer selectMappedBuffer = this.mappedFile.select(0, 16);
        ByteBuffer byteBuffer = selectMappedBuffer.getByteBuffer();

        int magic = byteBuffer.getInt();
//        如果魔数不一致说明这个文件可能损坏或者是一个新文件
//        TODO 文件损坏检查
        if (MESSAGE_MAGIC_CODE != magic) {
            long currentTimeMillis = System.currentTimeMillis();
            byteBuffer.rewind();
            byteBuffer.putInt(MESSAGE_MAGIC_CODE);
            byteBuffer.putInt(16);
            byteBuffer.putLong(currentTimeMillis);
            writePos = new AtomicInteger(16);
            latestWriteTimeStamp = new AtomicLong(currentTimeMillis);
            return;
        }

        writePos = new AtomicInteger(byteBuffer.getInt());
        latestWriteTimeStamp = new AtomicLong(byteBuffer.getLong());

    }

    public void shutdown() {

        saveHeader();

        if (selectMappedBuffer != null) {
            selectMappedBuffer.release();
            selectMappedBuffer = null;
        }
        if (mappedFile != null) {
            mappedFile.shutdown();
            mappedFile = null;
        }
    }

    /**
     * 添加消息
     * TODO 判断这个文件是否写的下这个消息
     *
     * @param message
     */
    public MessageExt addMessage(Message message) {

        if (message.getSize() > this.selectMappedBuffer.getByteBuffer().remaining()) {
//            TODO 自定义ex
            throw new RuntimeException("此log文件剩余空间不足，请切换到下一个log文件");
        }

//        获取序列化数据
        ByteBuffer messageByteBuffer = message.serialize();

//        写入消息
//        TODO 刷盘政策
        this.selectMappedBuffer.getByteBuffer()
            .put(messageByteBuffer);

//        更新写指针
        updateWritePos(messageByteBuffer.limit());

        return new MessageExt(
            message,
//            此文件的起始偏移 加上写的偏移
            this.startOffset + (writePos.get() - messageByteBuffer.limit())
        );

    }

    public Message getMessage(long commitLogOffset) {
        SelectMappedBuffer selectMappedBuffer = this.mappedFile.select(commitLogOffset - startOffset);
        try {
            ByteBufferStoreItem deserialize = StoreItemUtil.deserializeWithMovePost(
                selectMappedBuffer.getByteBuffer(),
                ByteBufferStoreItem::deserialize
            );
            return Message.deserialize(deserialize.body());
        } finally {
            selectMappedBuffer.release();
        }
    }

    public boolean isCanWrite(int messageSize) {
        return messageSize <= this.selectMappedBuffer.getByteBuffer().remaining();
    }

    /**
     * 更新写指针
     *
     * @param size 写入数据大小
     */
    private void updateWritePos(int size) {
//        设置指针和最后时间
        this.writePos.addAndGet(size);
        this.latestWriteTimeStamp.set(System.currentTimeMillis());
        saveHeader();
    }

    /**
     * 保存写指针
     */
    private void saveHeader() {
        ByteBuffer byteBuffer = writePosSelectMappedBuffer.getByteBuffer();
        byteBuffer.putInt(this.writePos.intValue());
        byteBuffer.putLong(this.latestWriteTimeStamp.longValue());
        byteBuffer.rewind();
    }

}
