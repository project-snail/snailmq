package com.snail.consumer.ack.handler.impl;

import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.ack.handler.AckAbstractHandler;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.RemotingCommandFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler.impl
 * @Description:
 * @date: 2020/12/24
 */
@Slf4j
public class CountTimeHandler extends AckAbstractHandler {

    private Integer ackTimeSeconds;

    private Integer ackCount;

    private static ScheduledExecutorService scheduledExecutorService;

    private AtomicInteger count = new AtomicInteger();

    private Long offset;

    private Long lastAckOffset;

    public CountTimeHandler() {
        if (scheduledExecutorService == null) {
            synchronized (CountTimeHandler.class) {
                if (scheduledExecutorService == null) {
                    scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("ConsumerClientServiceScheduledExecutor", true)
                    );
                }
            }
        }
    }

    @Override
    public void init(ConsumerClientService service, String topic, String group, int queueId) {
        super.init(service, topic, group, queueId);
        ackTimeSeconds = this.service.getConsumerClientConfig().getAckTimeSeconds();
        ackCount = this.service.getConsumerClientConfig().getAckCount();
        scheduledExecutorService.scheduleWithFixedDelay(
            () -> {
                if (offset == null || offset < 0 || (lastAckOffset != null && lastAckOffset.equals(offset))) {
                    return;
                }
                super.ack(offset);
                log.info("{}-{}-{} ack offset->{}", topic, group, queueId, offset);
                count.set(0);
            },
            ackTimeSeconds,
            ackTimeSeconds,
            TimeUnit.SECONDS
        );
    }

    @Override
    public void ack(long offset) {
        this.offset = offset;
        if (count.incrementAndGet() % ackCount == 0) {
            super.ack(offset);
            lastAckOffset = offset;
            log.info("{}-{}-{} ack offset->{}", topic, group, queueId, offset);
        }
    }
}
