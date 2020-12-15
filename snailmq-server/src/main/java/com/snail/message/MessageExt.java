package com.snail.message;

import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/14
 */
@Data
public class MessageExt {

    //在commitLog文件中的偏移量
    private long commitLogOffset;

    //源消息
    private Message message;

    public MessageExt(Message message, long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
        this.message = message;
    }

    public MessageExt(long commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
    }
}
