package com.snail.consumer.ack.handler;

import com.snail.consumer.ConsumerClientService;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler
 * @Description:
 * @date: 2020/12/24
 */
public interface AckHandler {

    void init(ConsumerClientService service, String topic, String group, int queueId);

    void ack(long offset);

}
