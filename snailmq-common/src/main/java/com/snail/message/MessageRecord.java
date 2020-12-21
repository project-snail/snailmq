package com.snail.message;

import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

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

    public MessageRecord(Message message) {
        this.message = message;
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

}
