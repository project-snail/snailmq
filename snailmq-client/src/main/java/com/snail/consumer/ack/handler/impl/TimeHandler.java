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

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler.impl
 * @Description:
 * @date: 2020/12/24
 */
@Slf4j
public class TimeHandler extends AckAbstractHandler {

    private Integer ackTimeSeconds;

    private static ScheduledExecutorService scheduledExecutorService;

    private Long offset;

    public TimeHandler() {
        if (scheduledExecutorService == null) {
            synchronized (TimeHandler.class) {
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
        this.ackTimeSeconds = this.service.getConsumerClientConfig().getAckTimeSeconds();
        scheduledExecutorService.scheduleWithFixedDelay(
            () -> {
                if (offset == null || offset < 0) {
                    return;
                }
                super.ack(offset);
                log.info("{}-{}-{} ack offset->{}", topic, group, queueId, offset);
            },
            ackTimeSeconds,
            ackTimeSeconds,
            TimeUnit.SECONDS
        );
    }

    @Override
    public void ack(long offset) {
        this.offset = offset;
    }
}
