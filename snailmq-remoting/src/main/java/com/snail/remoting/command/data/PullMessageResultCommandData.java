package com.snail.remoting.command.data;

import com.snail.message.MessageRes;

import java.nio.ByteBuffer;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/17
 */
public class PullMessageResultCommandData extends RemotingCommandData<PullMessageResultCommandData> {

    private final MessageRes messageRes;

    public PullMessageResultCommandData(MessageRes messageRes) {
        this.messageRes = messageRes;
    }

    @Override
    public ByteBuffer serialize() {
        return messageRes.serialize();
    }

    @Override
    public int getSize() {
        return messageRes.getSize();
    }

    public MessageRes getMessageRes() {
        return messageRes;
    }

    public static PullMessageResultCommandData deserialize(ByteBuffer byteBuffer) {
        return new PullMessageResultCommandData(MessageRes.deserialize(byteBuffer));
    }

}