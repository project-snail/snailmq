package com.snail.config.template;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snail.consumer.ConsumerClientService;
import com.snail.message.Message;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config.template
 * @Description:
 * @date: 2020/12/23
 */
public class SnailMqTemplate {

    @Autowired
    private ConsumerClientService consumerClientService;

    public void send(String topic, String key, String content, boolean isSync) {
        consumerClientService.pushMessage(buildMessage(topic, content, key), isSync);
    }

    public void send(String topic, Object content, String key, boolean isSync) {
        if (content == null) {
            throw new IllegalArgumentException("内容不能为null");
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            consumerClientService.pushMessage(buildMessage(topic, mapper.writeValueAsString(content), key), isSync);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("序列化内容失败", e);
        }
    }

    public void send(String topic, String content, boolean isSync) {
        send(topic, "", content, isSync);
    }

    public void send(String topic, Object content, boolean isSync) {
        send(topic, content, "", isSync);
    }

    private Message buildMessage(String topic, String content, String key) {

        if (StringUtils.isBlank(topic)) {
            throw new IllegalArgumentException("主题不能为空");
        }

        Message message = new Message();
        message.setTopic(topic);
        message.setFlag(0);
        message.setKey(Optional.ofNullable(key).orElse(""));
        message.setBody(ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8)));
        return message;
    }

}
