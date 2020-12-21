package com.snail.remoting.command.data;

import com.snail.message.Message;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/19
 */
public class MessageCommandData extends RemotingCommandData<MessageCommandData> {

    private final Message message;

    public MessageCommandData(Message message) {
        this.message = message;
    }

    @Override
    public ByteBuffer serialize() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(getSize());

        byteBuffer.putInt(message.getSize());

        byteBuffer.put(message.serialize());

        byteBuffer.flip();

        return byteBuffer.slice();
    }

    @Override
    public int getSize() {
        return 4 + message.getSize();
    }

    public static PushMessageCommandData deserialize(ByteBuffer byteBuffer) {

        byteBuffer = byteBuffer.slice();

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        return new PushMessageCommandData(Message.deserialize(byteBuffer));

    }

    public Message getMessage() {
        return message;
    }
}
