package com.snail.message;

import com.snail.store.*;
import com.snail.util.StoreItemUtil;
import lombok.Data;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @version V1.0
 * @author: csz
 * @Title 消息对象 带有序列化和反序列化
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/13
 */
@Data
public class Message implements StoreItem<Message> {

    //    主题
    private String topic;

    //    客户端自定义key
    private String key;

    //    标识号
    private int flag;

    //    消息数据
    private ByteBuffer body;

    //    序列化数据cache
    private ByteBuffer serialize;

    @Override
    public Message body() {
        return this;
    }

    public ByteBuffer serialize() {

        if (serialize != null) {
            return serialize.slice();
        }

        IntStoreItem flagStoreItem = new IntStoreItem(flag);

        StringStoreItem topicStoreItem = new StringStoreItem(topic);

        StringStoreItem keyStoreItem = new StringStoreItem(key);

        ByteBufferStoreItem bodyStoreItem = new ByteBufferStoreItem(body);

        int totalLen =
            flagStoreItem.getSize()
                + topicStoreItem.getSize()
                + keyStoreItem.getSize()
                + bodyStoreItem.getSize();

        ByteBuffer byteBuffer = ByteBuffer.allocateDirect(
            4 + totalLen
        );

        byteBuffer.putInt(totalLen);
        byteBuffer.putInt(flag);
        byteBuffer.put(topicStoreItem.serialize());
        byteBuffer.put(keyStoreItem.serialize());
        byteBuffer.put(bodyStoreItem.serialize());

        byteBuffer.flip();

        this.serialize = byteBuffer.slice();

        return byteBuffer.slice();

    }

    @Override
    public int getSize() {
        return Optional.ofNullable(this.serialize).orElseGet(this::serialize).limit();
    }

    public static Message deserialize(ByteBuffer byteBuffer) {

        Message message = new Message();

        IntStoreItem flagStoreItem = StoreItemUtil.deserializeWithMovePost(
            byteBuffer, IntStoreItem::deserialize
        );

        StringStoreItem topicStoreItem = StoreItemUtil.deserializeWithMovePost(
            byteBuffer, StringStoreItem::deserialize
        );

        StringStoreItem keyStoreItem = StoreItemUtil.deserializeWithMovePost(
            byteBuffer, StringStoreItem::deserialize
        );

        ByteBufferStoreItem bodyStoreItem = StoreItemUtil.deserializeWithMovePost(
            byteBuffer, ByteBufferStoreItem::deserialize
        );

        message.setFlag(flagStoreItem.body());
        message.setTopic(topicStoreItem.body());
        message.setKey(keyStoreItem.body());
        message.setBody(bodyStoreItem.body());

        return message;

    }

    public ByteBuffer getBody() {
        return body.slice();
    }

    @Override
    public String toString() {
        return "Message{" +
            "topic='" + topic + '\'' +
            ", key='" + key + '\'' +
            ", flag=" + flag +
            ", body=" + body.limit() +
            '}';
    }
}
