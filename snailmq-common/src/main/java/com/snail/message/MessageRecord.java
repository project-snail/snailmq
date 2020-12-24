package com.snail.message;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/17
 */
public class MessageRecord {

    private Message message;

    private long offset;

    private Consumer<Long> ackFun;

    public MessageRecord(Message message, long offset, Consumer<Long> ackFun) {
        this.message = message;
        this.offset = offset;
        this.ackFun = ackFun;
    }

    public Message getMessage() {
        return message;
    }

    public String getBody() {
        return Optional.ofNullable(message)
            .map(Message::getBody)
            .map(StandardCharsets.UTF_8::decode)
            .map(CharBuffer::toString)
            .orElse(null);
    }

    public void ack() {
        ackFun.accept(offset);
    }

}
