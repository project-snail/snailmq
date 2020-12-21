package com.snail.message;

import com.snail.store.AbstractStoreItem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageRes extends AbstractStoreItem<MessageRes> {

    //    源消息
    private Message message;

    //    下一个消息的偏移量 没有时为-1
    private long nextMsgOffset;

    @Override
    public MessageRes body() {
        return this;
    }

    @Override
    public ByteBuffer serialize() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(getSize());

        byteBuffer.putInt(8 + message.getSize());

        byteBuffer.putLong(nextMsgOffset);

        byteBuffer.put(message.serialize());

        byteBuffer.flip();

        return byteBuffer.slice();

    }

    @Override
    public int getSize() {
        return 4 + 8 + message.getSize();
    }

    public static MessageRes deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        long nextMsgOffset = byteBuffer.getLong();

        Message message = Message.deserialize(byteBuffer);

        return new MessageRes(message, nextMsgOffset);

    }

}
