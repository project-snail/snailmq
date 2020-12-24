package com.snail.consumer.ack.handler.impl;

import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.ack.handler.AckAbstractHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler.impl
 * @Description:
 * @date: 2020/12/24
 */
@Slf4j
public class CountHandler extends AckAbstractHandler {

    private Integer ackCount;

    private AtomicInteger count = new AtomicInteger();

    @Override
    public void init(ConsumerClientService service, String topic, String group, int queueId) {
        super.init(service, topic, group, queueId);
        this.ackCount = this.service.getConsumerClientConfig().getAckCount();
    }

    @Override
    public void ack(long offset) {
        if (count.incrementAndGet() % ackCount == 0) {
            super.ack(offset);
            log.info("{}-{}-{} ack offset->{}", topic, group, queueId, offset);
        }
    }
}
