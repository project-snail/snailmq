package com.snail.commit;

import com.snail.mapped.MappedFile;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.commit
 * @Description:
 * @date: 2021/01/05
 */
public interface FlushDiskHandler {
    /**
     * 执行刷盘
     * @param mappedFile 目标文件
     */
    void flush(MappedFile mappedFile);
}
