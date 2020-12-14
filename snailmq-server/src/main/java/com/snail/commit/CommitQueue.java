package com.snail.commit;

import com.snail.config.MessageStoreConfig;
import com.snail.mapped.MappedFile;
import com.snail.mapped.SelectMappedBuffer;
import com.snail.message.MessageExt;
import com.snail.store.IntStoreItem;
import com.snail.store.LongStoreItem;

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

    //    写指针
    private AtomicInteger writePos;

    //    log文件映射对象
    private MappedFile mappedFile;

    //    log文件区域映射对象
    private SelectMappedBuffer selectMappedBuffer;

    //    用于持久化头部的buffer
    private SelectMappedBuffer headerSelectMappedBuffer;

    private final static int QUEUE_ITEM_SIZE = 20;

    public CommitQueue(File file, int maxQueueItemSize, boolean autoCreate) throws IOException {
        this.mappedFile = new MappedFile(file, (maxQueueItemSize + 1) * QUEUE_ITEM_SIZE, autoCreate);
        init();
        this.selectMappedBuffer = this.mappedFile.select(writePos.get(), (maxQueueItemSize * QUEUE_ITEM_SIZE));
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
            byteBuffer.putInt(magic);
//            写指针
            byteBuffer.putInt(20);
            this.writePos = new AtomicInteger(0);
            byteBuffer.rewind();
        }

    }

    public void addMessageExt(MessageExt messageExt) {

        if (this.selectMappedBuffer.getByteBuffer().remaining() == 0) {
            //            TODO 自定义ex
            throw new RuntimeException("此queue文件剩余空间不足，请切换到下一个queue文件");
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(QUEUE_ITEM_SIZE);

        IntStoreItem logOffsetStoreItem = new IntStoreItem(messageExt.getCommitLogOffset());

        LongStoreItem currentTimeStoreItem = new LongStoreItem(System.currentTimeMillis());

        byteBuffer.put(logOffsetStoreItem.serialize());
        byteBuffer.put(currentTimeStoreItem.serialize());

        this.selectMappedBuffer.getByteBuffer().put(byteBuffer);

        updateWritePos();

    }

    /**
     * 更新写指针
     */
    private void updateWritePos() {
//        设置指针
        this.writePos.addAndGet(CommitQueue.QUEUE_ITEM_SIZE);
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

}
