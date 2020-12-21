package com.snail.exception;

import com.snail.commit.CommitQueue;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.exceeption
 * @Description:
 * @date: 2020/12/15
 */
public class CommitQueueOverflowException extends SnailBaseException {
    public CommitQueueOverflowException(CommitQueue commitQueue) {
        super(
            String.format(
                "此queue文件剩余空间不足，请切换到下一个queue文件 topic: %s queueId: %d offset: %d",
                commitQueue.getTopic(), commitQueue.getQueueId(), commitQueue.getStartOffset()
            )
        );
    }
}
