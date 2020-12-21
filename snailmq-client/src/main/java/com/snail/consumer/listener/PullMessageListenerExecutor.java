package com.snail.consumer.listener;

import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.MessageRecord;
import com.snail.message.MessageRes;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.RemotingCommandFactory;
import com.snail.remoting.command.data.PullMessageResultCommandData;
import com.snail.request.GetMessageRequest;
import com.snail.request.UpdateOffsetRequest;
import com.snail.result.RebalanceResult;
import lombok.extern.slf4j.Slf4j;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.listener
 * @Description:
 * @date: 2020/12/19
 */
@Slf4j
public class PullMessageListenerExecutor implements Runnable {

    private final ConsumerClientService service;

    private final RebalanceResult rebalanceResult;

    private final PullMessageListener listener;

    private GetMessageRequest getMessageRequest;

    private TopicGroupConsumerOffset consumerOffset;

    private volatile boolean isRunning = false;

    public PullMessageListenerExecutor(ConsumerClientService service, RebalanceResult rebalanceResult, PullMessageListener listener, TopicGroupConsumerOffset nowOffset) {
        this.service = service;
        this.rebalanceResult = rebalanceResult;
        this.listener = listener;
        this.consumerOffset = nowOffset;
        getMessageRequest = new GetMessageRequest();
        getMessageRequest.setTopic(rebalanceResult.getTopic());
        getMessageRequest.setGroup(rebalanceResult.getGroup());
        getMessageRequest.setQueueId(rebalanceResult.getQueueId());
        getMessageRequest.setOffset(consumerOffset.getNextMsgOffset());
        getMessageRequest.setVersion(rebalanceResult.getVersion());
    }

    @Override
    public void run() {

        this.isRunning = true;

//        TODO 为-1时处理
        while (consumerOffset.getNextMsgOffset() != -1) {

            RemotingCommand remotingCommand = RemotingCommandFactory.pullMessage(getMessageRequest);
            RemotingCommand res = this.service.sendSync(remotingCommand);

            if (res == null) {
                continue;
            }

            if (res.isOK()) {
                handleMessage(res);
                if (consumerOffset.getNextMsgOffset() != -1) {
                    this.consumerOffset.setLastOffset(consumerOffset.getNextMsgOffset());
                    ack();
                }
                continue;
            }

            switch (res.getExceptionState()) {
                case QUEUE_NO_PERMISSIONS:
                    log.warn("目标queue无权限，退出消费");
                case OFFSET_OVERFLOW:
                    log.error("消费偏移量异常", this.consumerOffset);
                case MESSAGE_GONE:
                case MESSAGE_MISSING:
                    log.warn("server出现消息丢失错误 {} offset {}", rebalanceResult.toString(), this.consumerOffset);
                    this.isRunning = false;
                    service.removeListenerExecutor(listener, this);
                    return;
            }

        }

        this.isRunning = false;
        service.removeListenerExecutor(listener, this);

    }

    private void ack() {

//        ack策略
        UpdateOffsetRequest request = new UpdateOffsetRequest();
        request.setTopic(this.consumerOffset.getTopic());
        request.setGroup(this.consumerOffset.getGroup());
        request.setQueueId(this.consumerOffset.getQueueId());
        request.setOffset(this.consumerOffset.getLastOffset());

        RemotingCommand remotingCommand = RemotingCommandFactory.updateOffset(request);
        this.service.sendAsync(remotingCommand);
    }

    private void handleMessage(RemotingCommand res) {

        PullMessageResultCommandData deserialize = PullMessageResultCommandData.deserialize(res.getDataByteBuffer());

        MessageRes messageRes = deserialize.getMessageRes();

        MessageRecord messageRecord = new MessageRecord(messageRes.getMessage());

        this.listener.listener(messageRecord);

        this.consumerOffset.setNextMsgOffset(messageRes.getNextMsgOffset());

        this.getMessageRequest.setOffset(messageRes.getNextMsgOffset());

    }

    public boolean isRunning() {
        return isRunning;
    }

    public int getVersion() {
        return this.getMessageRequest.getVersion();
    }
}
