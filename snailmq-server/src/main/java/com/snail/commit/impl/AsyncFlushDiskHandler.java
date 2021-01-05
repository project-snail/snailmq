package com.snail.commit.impl;

import com.snail.commit.FlushDiskHandler;
import com.snail.mapped.MappedFile;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.commit.impl
 * @Description: 同步刷盘策略
 * @date: 2021/01/05
 */
@Slf4j
public class AsyncFlushDiskHandler implements FlushDiskHandler {

    private final Integer flushDishInterval;

    private final ScheduledExecutorService scheduledExecutorService;

    private Set<MappedFile> mappedFileSet = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private ReadWriteLock lock = new ReentrantReadWriteLock();

    public AsyncFlushDiskHandler(ScheduledExecutorService scheduledExecutorService, Integer flushDishInterval) {
        this.flushDishInterval = flushDishInterval;
        this.scheduledExecutorService = scheduledExecutorService;
        init();
    }

    private void init() {
        scheduledExecutorService.scheduleWithFixedDelay(
            this::doFlush,
            flushDishInterval,
            flushDishInterval,
            TimeUnit.MILLISECONDS
        );
    }

    private void doFlush() {
        List<MappedFile> mappedFileList;
        Lock writeLock = this.lock.writeLock();
        try {
            writeLock.lock();
            mappedFileList = new ArrayList<>(mappedFileSet);
            mappedFileSet.clear();
        } finally {
            writeLock.unlock();
        }
        for (MappedFile mappedFile : mappedFileList) {
            try {
                mappedFile.getFileChannel().force(false);
            } catch (IOException e) {
                log.error("刷盘时发生异常: ", e);
            }
        }
    }

    @Override
    public void flush(MappedFile mappedFile) {
        if (mappedFileSet.contains(mappedFile)) {
            return;
        }
        Lock readLock = this.lock.readLock();
        try {
            readLock.lock();
            mappedFileSet.add(mappedFile);
        } finally {
            readLock.unlock();
        }
    }
}
