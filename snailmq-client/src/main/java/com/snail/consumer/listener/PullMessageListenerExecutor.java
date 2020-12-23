package com.snail.consumer.listener;

import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.MessageRecord;
import com.snail.message.MessageRes;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.RemotingCommandFactory;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.remoting.command.data.PullMessageResultCommandData;
import com.snail.remoting.command.exception.SyncRemotingCommandTimeOutException;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.request.GetMessageRequest;
import com.snail.request.PullNextMessageOffsetRequest;
import com.snail.request.UpdateOffsetRequest;
import com.snail.result.RebalanceResult;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;


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

        try {
            while (this.isRunning && !Thread.currentThread().isInterrupted()) {
                doPull();
            }
        } finally {
            this.isRunning = false;
            service.removeListenerExecutor(listener, this);
        }

    }

    private void doPull() {

//        如果没有下一条消息了
        if (consumerOffset.getNextMsgOffset() == -1) {
//            启动推模式拉取下一条偏移量
            pullNextMessageOffset();
        }

//        还是没有, 退出
        if (getMessageRequest.getOffset() == -1) {
            this.isRunning = false;
        }

        RemotingCommand remotingCommand = RemotingCommandFactory.pullMessage(getMessageRequest);
        RemotingCommand res;
        try {
            res = this.service.sendSync(remotingCommand, 30, TimeUnit.SECONDS);
        } catch (SyncRemotingCommandTimeOutException ignored) {
            return;
        }

        if (res == null) {
            return;
        }

        if (res.isOK()) {
            handleMessage(res);
            ack();
            return;
        }

        switch (res.getExceptionState()) {
            case QUEUE_NO_PERMISSIONS:
                log.warn("目标queue无权限，退出消费");
            case OFFSET_OVERFLOW:
                log.error("消费偏移量异常", this.consumerOffset);
            case MESSAGE_GONE:
            case MESSAGE_MISSING:
                log.warn("server出现消息丢失错误 {} offset {}", rebalanceResult.toString(), this.consumerOffset);
                throw new RuntimeException("拉取消息异常" + res.getExceptionState().name());
        }
    }

    private void pullNextMessageOffset() {

        SyncRemotingCommand syncRemotingCommand = new SyncRemotingCommand();
        PullNextMessageOffsetRequest request = new PullNextMessageOffsetRequest(
            -1,
            this.consumerOffset.getTopic(),
            this.consumerOffset.getGroup(),
            this.consumerOffset.getQueueId(),
            this.consumerOffset.getLastOffset(),
            this.getVersion()
        );
        request.setSyncCode(syncRemotingCommand.getSyncCode());
        RemotingCommand remotingCommand = RemotingCommandFactory.pullNextMessageOffset(request);
        syncRemotingCommand.setBody(remotingCommand);

        RemotingCommand res;
//        等待5分钟
        try {
            res = this.service.sendSync(
                syncRemotingCommand, 5, TimeUnit.MINUTES
            );
        } catch (SyncRemotingCommandTimeOutException e) {
            return;
        }

        if (!res.isOK()) {
            if (CommandExceptionStateEnums.QUEUE_NO_PERMISSIONS.equals(res.getExceptionState())) {
                throw new RuntimeException("可能发送重平衡，失去该队列消费权限");
            }
            throw new RuntimeException("拉取消息异常" + res.getExceptionState().name());
        }

        JsonCommandData<JsonCommandData> deserialize = JsonCommandData.deserialize(res.getDataByteBuffer());
        TopicGroupConsumerOffset nextMsgOffset = deserialize.getObj(TopicGroupConsumerOffset.class);
        this.getMessageRequest.setOffset(nextMsgOffset.getNextMsgOffset());

    }

    private void ack() {

//        TODO ack策略
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

        this.consumerOffset.setLastOffset(this.getMessageRequest.getOffset());
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
