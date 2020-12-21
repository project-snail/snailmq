package com.snail.remoting.command.data;

import com.snail.message.Message;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/17
 */
public class PushMessageCommandData extends MessageCommandData {
    public PushMessageCommandData(Message message) {
        super(message);
    }
}
