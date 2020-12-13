package com.snail.mapped;


import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title 区域映射文件对象
 * @Package: com.snail.mapped
 * @date: 2020/12/13
 */
@Data
public class SelectMappedBuffer {

    private final long startOffset;

    private final long size;

    private final ByteBuffer byteBuffer;

    @Getter(AccessLevel.NONE)
    private MappedFile mappedFile;

    public SelectMappedBuffer(long startOffset, long size, ByteBuffer byteBuffer, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.size = size;
        this.byteBuffer = byteBuffer;
        this.mappedFile = mappedFile;
    }

    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }

}
