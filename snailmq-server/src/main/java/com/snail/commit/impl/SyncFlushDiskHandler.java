package com.snail.commit.impl;

import com.snail.commit.FlushDiskHandler;
import com.snail.mapped.MappedFile;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.commit.impl
 * @Description: 同步刷盘策略
 * @date: 2021/01/05
 */
@Slf4j
public class SyncFlushDiskHandler implements FlushDiskHandler {
    @Override
    public void flush(MappedFile mappedFile) {
        try {
            mappedFile.getFileChannel().force(false);
        } catch (IOException e) {
            log.error("刷盘时发生异常: ", e);
        }
    }
}
