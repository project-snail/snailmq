package com.snail.remoting.command;

import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.remoting.command.exception.SyncRemotingCommandTimeOutException;
import com.snail.store.AbstractStoreItem;
import com.snail.store.IntStoreItem;
import com.snail.util.StoreItemUtil;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description: 同步获取的command
 * @date: 2020/12/18
 */
public class SyncRemotingCommand extends AbstractStoreItem<RemotingCommand> {

    private static final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new NamedThreadFactory("SyncRemotingCommandClearScheduledExecutor", false)
    );

    private final static Map<Integer, SyncRemotingCommand> syncRemotingCommandMap = new ConcurrentHashMap<>();

    private final static ConcurrentSkipListMap<SyncRemotingCommand, Integer> syncRemotingCommandClearMap = new ConcurrentSkipListMap<>(
        Comparator.comparing(SyncRemotingCommand::getTime)
    );

    private static long syncMaxWaitTime = 15 * 60 * 1000;

    static {
//        清除过久无响应的请求
        scheduledExecutorService.scheduleWithFixedDelay(
            SyncRemotingCommand::clearUpSyncRequest,
            1,
            1,
            TimeUnit.MINUTES
        );
    }

    private final static AtomicInteger syncCodeGenerate = new AtomicInteger();

    private RemotingCommand body;

    private RemotingCommand res;

    private final Integer syncCode;

    private IntStoreItem syncCodeStoreItem;

    private CountDownLatch latch;

    private long time = System.currentTimeMillis();

    public SyncRemotingCommand() {
        this(null);
    }

    public SyncRemotingCommand(RemotingCommand body) {
        this.body = body;
        syncCode = syncCodeGenerate.incrementAndGet();
        this.syncCodeStoreItem = new IntStoreItem(syncCode);
        latch = new CountDownLatch(1);
        putCommand();
    }

    public SyncRemotingCommand(RemotingCommand res, IntStoreItem syncCodeStoreItem) {
        this.res = res;
        this.body = res;
        this.syncCodeStoreItem = syncCodeStoreItem;
        syncCode = syncCodeStoreItem.body();
    }

    @Override
    public RemotingCommand body() {
        return body;
    }

    public void setBody(RemotingCommand body) {
        this.body = body;
    }

    @Override
    public ByteBuffer serialize() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(getSize());

        byteBuffer.putInt(syncCodeStoreItem.getSize() + body.getSize());

        byteBuffer.put(syncCodeStoreItem.serialize());

        byteBuffer.put(body.serialize());

        byteBuffer.flip();

        return byteBuffer.slice();
    }

    @Override
    public int getSize() {
        return 4 + syncCodeStoreItem.getSize() + body.getSize();
    }

    public static SyncRemotingCommand deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        IntStoreItem syncCodeStoreItem = StoreItemUtil.deserializeWithMovePost(
            byteBuffer,
            IntStoreItem::deserialize
        );

        RemotingCommand remotingCommand = StoreItemUtil.deserializeWithMovePost(
            byteBuffer,
            RemotingCommand::deserialize
        );

        return new SyncRemotingCommand(remotingCommand, syncCodeStoreItem);

    }

    public void clearBody() {
        this.body = null;
    }

    public RemotingCommand getRes(long timeout, TimeUnit unit) throws InterruptedException {
        if (res == null) {
            boolean await = latch.await(timeout, unit);
            if (!await) {
                throw new SyncRemotingCommandTimeOutException();
            }
        }
        return res;
    }

    public RemotingCommand getRes() throws InterruptedException {
        if (res == null) {
            latch.await();
        }
        return res;
    }

    public void setRes(RemotingCommand res) {
        this.res = res;
        latch.countDown();
    }

    public Integer getSyncCode() {
        return syncCode;
    }

    public void pushRes() {
        SyncRemotingCommand syncRemotingCommand = getCommand(this.syncCode);

        if (syncRemotingCommand != null) {
            syncRemotingCommand.setRes(this.res);
            syncRemotingCommand.clearBody();
            syncRemotingCommandMap.remove(this.syncCode);
        }

    }

    public void putCommand() {
        syncRemotingCommandMap.put(this.syncCode, this);
    }

    public static SyncRemotingCommand getCommand(Integer syncCode) {
        return syncRemotingCommandMap.get(syncCode);
    }

    public long getTime() {
        return time;
    }

    public static long getSyncMaxWaitTime() {
        return syncMaxWaitTime;
    }

    public static void setSyncMaxWaitTime(long syncMaxWaitTime) {
        SyncRemotingCommand.syncMaxWaitTime = syncMaxWaitTime;
    }

    private static void clearUpSyncRequest() {
        Map.Entry<SyncRemotingCommand, Integer> firstEntry = syncRemotingCommandClearMap.firstEntry();
        while (firstEntry != null) {

            SyncRemotingCommand holder = firstEntry.getKey();

            long holderTime = holder.getTime();

//            如果这个请求已经超时没响应了 清除它
            if (holderTime > syncMaxWaitTime) {
                syncRemotingCommandClearMap.remove(holder);
                syncRemotingCommandMap.remove(firstEntry.getValue());
            }

            firstEntry = syncRemotingCommandClearMap.firstEntry();

        }
    }

}
