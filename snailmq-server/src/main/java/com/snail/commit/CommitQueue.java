package com.snail.commit;

import com.snail.config.MessageStoreConfig;
import com.snail.exception.CommitQueueOverflowException;
import com.snail.mapped.MappedFile;
import com.snail.mapped.SelectMappedBuffer;
import com.snail.message.MessageExt;
import com.snail.store.LongStoreItem;
import com.snail.util.StoreItemUtil;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.commit
 * @Description:
 * @date: 2020/12/14
 */
public class CommitQueue {

    private String topic;

    private Integer queueId;

    private Long startOffset;

    //    写指针
    private AtomicInteger writePos;

    //    log文件映射对象
    private MappedFile mappedFile;

    //    log文件区域映射对象
    private SelectMappedBuffer selectMappedBuffer;

    //    用于持久化头部的buffer
    private SelectMappedBuffer headerSelectMappedBuffer;

    public final static int QUEUE_ITEM_SIZE = 20;

    private final FlushDiskHandler flushDiskHandler;

    public CommitQueue(String topic, Integer queueId, Long startOffset, File file, int maxQueueItemSize, boolean autoCreate, FlushDiskHandler flushDiskHandler) throws IOException {
        this.topic = topic;
        this.queueId = queueId;
        this.startOffset = startOffset;
        this.flushDiskHandler = flushDiskHandler;
        this.mappedFile = new MappedFile(file, (maxQueueItemSize + 1) * QUEUE_ITEM_SIZE, autoCreate);
        init();
        this.selectMappedBuffer = this.mappedFile.select(
            writePos.get() * QUEUE_ITEM_SIZE,
            (maxQueueItemSize + 1 - writePos.get()) * QUEUE_ITEM_SIZE
        );
        this.headerSelectMappedBuffer = this.mappedFile.select(4, 16);
    }

    private void init() {

        SelectMappedBuffer header = this.mappedFile.select(0, 20);

        ByteBuffer byteBuffer = header.getByteBuffer();

        int magic = byteBuffer.getInt();

//        新文件或者文件损坏
//        TODO 文件校检
        if (magic != MessageStoreConfig.MESSAGE_MAGIC_CODE) {
            byteBuffer.rewind();
//            魔数
            byteBuffer.putInt(MessageStoreConfig.MESSAGE_MAGIC_CODE);
//            写指针
            byteBuffer.putInt(1);
            this.writePos = new AtomicInteger(1);
            byteBuffer.rewind();
            return;
        }

        this.writePos = new AtomicInteger(byteBuffer.getInt());

    }

    public MessageExt addMessageExt(MessageExt messageExt) {

        if (this.selectMappedBuffer.getByteBuffer().remaining() == 0) {
            throw new CommitQueueOverflowException(this);
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(QUEUE_ITEM_SIZE);

        LongStoreItem logOffsetStoreItem = new LongStoreItem(messageExt.getCommitLogOffset());

        LongStoreItem currentTimeStoreItem = new LongStoreItem(System.currentTimeMillis());

        byteBuffer.put(logOffsetStoreItem.serialize());
        byteBuffer.put(currentTimeStoreItem.serialize());
        byteBuffer.put(new byte[QUEUE_ITEM_SIZE - logOffsetStoreItem.getSize() - currentTimeStoreItem.getSize()]);

        byteBuffer.flip();

        this.selectMappedBuffer.getByteBuffer().put(byteBuffer);

        messageExt.setQueueId(this.queueId);
        messageExt.setQueueOffset(this.writePos.get());

        updateWritePos();

        return messageExt;
    }

    public MessageExt getMessageExt(Long offset) {
        SelectMappedBuffer selectMappedBuffer = this.mappedFile.select(
            ((offset - startOffset) + 1) * QUEUE_ITEM_SIZE,
            QUEUE_ITEM_SIZE
        );
        try {
            ByteBuffer byteBuffer = selectMappedBuffer.getByteBuffer();
            LongStoreItem logOffsetStoreItem = StoreItemUtil.deserializeWithMovePost(
                byteBuffer,
                LongStoreItem::deserialize
            );
            return new MessageExt(logOffsetStoreItem.body());
        } finally {
            selectMappedBuffer.release();
        }
    }

    /**
     * 更新写指针
     */
    private void updateWritePos() {
//        设置指针
        this.writePos.incrementAndGet();
        saveHeader();
    }

    /**
     * 保存写指针
     */
    private void saveHeader() {
        ByteBuffer byteBuffer = headerSelectMappedBuffer.getByteBuffer();
        byteBuffer.putInt(this.writePos.intValue());
        byteBuffer.rewind();
    }

    public boolean getIsWritable() {
        return this.selectMappedBuffer.getByteBuffer().remaining() > 0;
    }

    public Integer getQueueId() {
        return queueId;
    }

    public Long getStartOffset() {
        return startOffset;
    }

    public Integer getWritePos() {
        return writePos.get() - 1;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void shutdown() {
        this.selectMappedBuffer.release();
        this.headerSelectMappedBuffer.release();
        this.mappedFile.shutdown();
    }
}
