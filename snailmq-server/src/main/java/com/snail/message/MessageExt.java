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
public class MessageExt extends Message {

//    在commitLog文件中的偏移量
    private int commitLogOffset;

    public MessageExt(Message message, int commitLogOffset) {
        this.commitLogOffset = commitLogOffset;
        setTopic(message.getTopic());
        setKey(message.getKey());
        setFlag(message.getFlag());
        setBody(message.getBody());
    }
}
