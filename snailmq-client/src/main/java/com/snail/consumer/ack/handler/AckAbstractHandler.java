package com.snail.consumer.ack.handler;

import com.snail.consumer.ConsumerClientService;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.RemotingCommandFactory;
import com.snail.request.UpdateOffsetRequest;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler
 * @Description:
 * @date: 2020/12/24
 */
public abstract class AckAbstractHandler implements AckHandler {

    protected ConsumerClientService service;

    protected String topic;

    protected String group;

    protected int queueId;

    protected UpdateOffsetRequest request;

    @Override
    public void init(ConsumerClientService service, String topic, String group, int queueId) {
        this.service = service;
        this.topic = topic;
        this.group = group;
        this.queueId = queueId;
        this.request = new UpdateOffsetRequest();
        request.setTopic(topic);
        request.setGroup(group);
        request.setQueueId(queueId);
    }

    @Override
    public void ack(long offset) {
        this.request.setOffset(offset);
        RemotingCommand remotingCommand = RemotingCommandFactory.updateOffset(request);
        this.service.sendSync(remotingCommand);
    }
}
