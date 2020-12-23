package com.snail.consumer.impl;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.application.event.AddMessageEvent;
import com.snail.config.MessageStoreConfig;
import com.snail.consumer.ConsumerGroup;
import com.snail.consumer.MqService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.exception.SnailBaseException;
import com.snail.message.MessageExt;
import com.snail.message.RebalanceRequest;
import com.snail.consumer.rebalance.RebalanceResult;
import com.snail.consumer.rebalance.RebalanceService;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.request.GetMessageRequest;
import com.snail.message.Message;
import com.snail.message.MessageRes;
import com.snail.request.UpdateOffsetRequest;
import com.snail.store.CommitStore;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.impl
 * @Description:
 * @date: 2020/12/16
 */
@Service
public class MqServiceImpl implements MqService, InitializingBean {

    @Autowired
    private CommitStore commitStore;

    @Autowired
    private MessageStoreConfig messageStoreConfig;

    @Autowired
    private ConsumerGroup consumerGroup;

    @Autowired
    private RebalanceService rebalanceService;

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new NamedThreadFactory("MqServiceScheduledExecutor", false)
    );

    @Override
    public MessageRes getMessage(String topic, int queueId, long offset) {
        return commitStore.getMessage(topic, queueId, offset);
    }

    @Override
    public MessageRes getMessage(GetMessageRequest request) {

        boolean checkOwnerRes = rebalanceService.checkOwner(
            request.getCid(),
            request.getVersion(),
            request.getTopic(),
            request.getGroup(),
            request.getQueueId()
        );

        if (!checkOwnerRes) {
            throw new SnailBaseException("该queue已分配给其他用户", CommandExceptionStateEnums.QUEUE_NO_PERMISSIONS);
        }

        return getMessage(request.getTopic(), request.getQueueId(), request.getOffset());

    }

    @Override
    public List<TopicGroupConsumerOffset> getNextMsgOffset(List<TopicGroupOffset> topicGroupList) {
        return topicGroupList.stream()
            .map(
                topicGroupOffset -> new TopicGroupConsumerOffset(
                    topicGroupOffset.getTopic(),
                    topicGroupOffset.getGroup(),
                    topicGroupOffset.getQueueId(),
                    topicGroupOffset.getOffset(),
                    this.commitStore.getNextMsgOffset(
                        topicGroupOffset.getTopic(),
                        topicGroupOffset.getQueueId(),
                        topicGroupOffset.getOffset()
                    ).getNextMsgOffset()
                )
            )
            .collect(Collectors.toList());
    }

    @Override
    public TopicGroupConsumerOffset getNextMsgOffset(TopicGroupOffset topicGroup) {
        return Optional.ofNullable(getNextMsgOffset(Collections.singletonList(topicGroup)))
            .filter(CollectionUtil::isNotEmpty)
            .map(list -> list.get(0))
            .orElse(null);
    }

    @Override
    public void addMessage(Message message) {
        MessageExt messageExt = commitStore.addMessage(message);
//        通知写入
        applicationEventPublisher.publishEvent(
            new AddMessageEvent(this, messageExt)
        );
    }

    @Override
    public List<TopicGroupConsumerOffset> getOffset(List<TopicGroupOffset> topicGroupList) {

//        获取消费记录
        topicGroupList = consumerGroup.getOffset(topicGroupList);

//        获取下一条消息位置
        return getNextMsgOffset(topicGroupList);

    }

    private String makeATopicGroupQueueKey(TopicGroupOffset topicGroupOffset) {
        return topicGroupOffset.getTopic() + "_" + topicGroupOffset.getGroup() + "_" + topicGroupOffset.getQueueId();
    }

    @Override
    public void updateOffset(UpdateOffsetRequest updateOffsetRequest) {
        this.updateOffsetBatch(Collections.singletonList(updateOffsetRequest));
    }

    @Override
    public void updateOffsetBatch(List<UpdateOffsetRequest> updateOffsetRequestList) {
        for (UpdateOffsetRequest updateOffsetRequest : updateOffsetRequestList) {
            consumerGroup.updateOffset(
                updateOffsetRequest.getTopic(),
                updateOffsetRequest.getGroup(),
                updateOffsetRequest.getQueueId(),
                updateOffsetRequest.getOffset()
            );
        }
    }

    @Override
    public List<TopicGroupOffset> getQueueMinOffset(Collection<TopicGroupOffset> topicGroupOffsetList) {
        return commitStore.getQueueMinOffset(topicGroupOffsetList);
    }

    @Override
    public List<RebalanceResult> registerCid(String cid, List<RebalanceRequest> rebalanceRequestList) {
        rebalanceService.addCid(cid, rebalanceRequestList);
        return rebalanceService.getRebalanceResult(cid, rebalanceRequestList);
    }

    @Override
    public void removeCid(String cid) {
        rebalanceService.removeCid(cid);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        scheduledTask();
    }

    private void scheduledTask() {
        scheduledExecutorService.scheduleAtFixedRate(
            consumerGroup::persistence,
            1000 * 5,
            this.messageStoreConfig.getPersistenceConsumerGroupOffsetInterval(),
            TimeUnit.MILLISECONDS
        );
    }

}
