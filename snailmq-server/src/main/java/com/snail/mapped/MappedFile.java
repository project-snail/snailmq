package com.snail.mapped;

import lombok.extern.slf4j.Slf4j;
import sun.misc.Cleaner;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version V1.0
 * @author: csz
 * @Title 映射文件对象
 * @Package: rocketmq
 * @date: 2020/12/12
 */
@Slf4j
public class MappedFile {

    private File file;

    private FileChannel fileChannel;

    private MappedByteBuffer mappedByteBuffer;

    private long fileSize;

    private volatile boolean available = true;

    //    总共映射了多少文件
    private final static AtomicInteger TOTAL_MAPPED_FILE_SIZE = new AtomicInteger();
    //    总共映射了多少内存
    private final static AtomicLong TOTAL_VIRTUAL_MEMORY_SIZE = new AtomicLong();

    public MappedFile(File file, long size) throws IOException {
        this(file, size, false);
    }

    public MappedFile(File file, long size, boolean autoCreate) throws IOException {

        if (file == null) {
            throw new IllegalArgumentException("文件不得为空");
        }

        if (size < 0) {
            throw new IllegalArgumentException("size不得小于0");
        }

        if (!file.exists()) {
            if (!autoCreate) {
                throw new IllegalArgumentException("文件不存在");
            }
            buildFile(file);
        }

        init(file, size);
    }

    /**
     * 初始化对象 文件对象 文件大小 文件映射
     *
     * @param file 文件对象
     * @param size 文件映射大小
     */
    private void init(File file, long size) throws IOException {
        this.file = file;
        this.fileSize = size;
        this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
//        初始化文件映射
        this.mappedByteBuffer = fileChannel
            .map(FileChannel.MapMode.READ_WRITE, 0, size);

        log.info(
            "增加文件映射 目前共 {} 个文件对象, 共 {}MB 虚拟内存",
            TOTAL_MAPPED_FILE_SIZE.incrementAndGet(),
            TOTAL_VIRTUAL_MEMORY_SIZE.addAndGet(size) / (1024 * 1024)
        );
    }

    /**
     * 从指定的位置创建size大小的映射
     *
     * @param pos  起始偏移值
     * @param size 映射大小
     * @return ByteBuffer
     */
    public SelectMappedBuffer select(long pos, int size) {
        if (pos > fileSize || pos + size > fileSize) {
            throw new IllegalArgumentException("偏移量错误, pos和pos+size不得大于文件大小");
        }
        if (size < 0) {
            throw new IllegalArgumentException("映射大小错误，大小不得小于0");
        }
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        byteBuffer.position(Math.toIntExact(pos));
        byteBuffer.limit(Math.toIntExact(pos + size));
        return new SelectMappedBuffer(pos, size, byteBuffer.slice(), this);
    }

    /**
     * 创建从指定位置到文件尾部的映射
     *
     * @param pos 起始偏移值
     * @return ByteBuffer
     */
    public SelectMappedBuffer select(long pos) {
        return select(pos, Math.toIntExact(this.fileSize - pos));
    }

    /**
     * 构建新文件
     *
     * @param file 文件对象
     */
    private void buildFile(File file) throws IOException {
        if (!file.getParentFile().exists()) {
            boolean mkdirsResult = file.getParentFile().mkdirs();
            log.info("创建新文件的父级文件夹 {} {}", file.getParentFile().getAbsolutePath(), (mkdirsResult ? "OK" : "Failed"));
        }
        if (!file.exists()) {
            boolean createNewFile = file.createNewFile();
            log.info("创建新文件 {} {}", file.getName(), (createNewFile ? "OK" : "Failed"));
        }
    }

    public void shutdown() {

        setAvailable(false);

        release();

    }

    public void release() {

        if (isAvailable()) {
            return;
        }

        String fileName = Optional.ofNullable(file).map(File::getName).orElse("unknown");

        try {
            this.fileChannel.force(false);
        } catch (IOException e) {
            log.error(String.format("fileName: %s 刷盘失败", fileName), e);
        }

        try {
            clean(this.mappedByteBuffer);
            this.mappedByteBuffer = null;
        } catch (Exception e) {
            log.error(String.format("fileName: %s 关闭映射文件异常", fileName), e);
        }

        log.info(
            "当前文件映射对象关闭 目前剩余 共 {} 个文件对象, 共 {}MB 虚拟内存",
            TOTAL_MAPPED_FILE_SIZE.addAndGet(-1),
            TOTAL_VIRTUAL_MEMORY_SIZE.addAndGet(this.fileSize * -1) / (1024 * 1024)
        );

    }

    private void clean(final MappedByteBuffer buffer) throws Exception {
        AccessController.doPrivileged(
            (PrivilegedAction<MappedByteBuffer>) () -> {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner");
                    getCleanerMethod.setAccessible(true);
                    Cleaner cleaner = (Cleaner) getCleanerMethod.invoke(buffer, new Object[0]);
                    cleaner.clean();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        );
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public FileChannel getFileChannel() {
        return fileChannel;
    }
}
