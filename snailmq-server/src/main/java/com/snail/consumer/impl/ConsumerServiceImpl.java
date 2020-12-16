package com.snail.consumer.impl;

import com.snail.config.MessageStoreConfig;
import com.snail.consumer.ConsumerGroup;
import com.snail.consumer.ConsumerService;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.Message;
import com.snail.message.MessageRes;
import com.snail.store.CommitStore;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
public class ConsumerServiceImpl implements ConsumerService, InitializingBean {

    @Autowired
    private CommitStore commitStore;

    @Autowired
    private MessageStoreConfig messageStoreConfig;

    @Autowired
    private ConsumerGroup consumerGroup;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        run -> new Thread("ConsumerServiceSingleThreadScheduledExecutor")
    );

    @Override
    public MessageRes getMessage(String topic, int queueId, long offset) {
        return commitStore.getMessage(topic, queueId, offset);
    }

    @Override
    public List<TopicGroupOffset> getNextMsgOffset(List<TopicGroupOffset> topicGroupList) {
        return topicGroupList.stream()
            .map(
                topicGroupOffset -> new TopicGroupOffset(
                    topicGroupOffset.getTopic(),
                    topicGroupOffset.getGroup(),
                    topicGroupOffset.getQueueId(),
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
    public void addMessage(Message message) {
        commitStore.addMessage(message);
    }

    @Override
    public List<TopicGroupOffset> getOffset(List<TopicGroupOffset> topicGroupList) {

//        获取消费记录
        return consumerGroup.getOffset(topicGroupList);

//        让客户端主动去拿消费记录 因为这里给出的是上一次消费的最后一条消息offset
//
////        筛选出没有消费过的记录
//        Map<String, TopicGroupOffset> topicGroupOffsetMapOfNoOffset = consumerGroupOffset.stream()
//            .filter(topicGroupOffset -> Long.valueOf(-1).equals(topicGroupOffset.getOffset()))
//            .collect(Collectors.toMap(this::makeATopicGroupQueueKey, Function.identity()));
//
//        if (topicGroupOffsetMapOfNoOffset.isEmpty()) {
//            return consumerGroupOffset;
//        }
//
////        找到该queue的最小记录
//        List<TopicGroupOffset> queueMinOffset = getQueueMinOffset(topicGroupOffsetMapOfNoOffset.values());
//
////        赋予它最小记录
//        for (TopicGroupOffset topicGroupOffset : queueMinOffset) {
//            TopicGroupOffset groupOffset = topicGroupOffsetMapOfNoOffset.get(
//                makeATopicGroupQueueKey(topicGroupOffset)
//            );
//            if (groupOffset == null) {
//                continue;
//            }
//            groupOffset.setOffset(topicGroupOffset.getOffset());
//        }
//
//        return consumerGroupOffset;
    }

    private String makeATopicGroupQueueKey(TopicGroupOffset topicGroupOffset) {
        return topicGroupOffset.getTopic() + "_" + topicGroupOffset.getGroup() + "_" + topicGroupOffset.getQueueId();
    }

    @Override
    public void updateOffset(TopicGroupOffset topicGroupOffset) {
        this.updateOffsetBatch(Collections.singletonList(topicGroupOffset));
    }

    @Override
    public void updateOffsetBatch(List<TopicGroupOffset> topicGroupOffsetList) {
        for (TopicGroupOffset topicGroupOffset : topicGroupOffsetList) {
            consumerGroup.updateOffset(
                topicGroupOffset.getTopic(),
                topicGroupOffset.getGroup(),
                topicGroupOffset.getQueueId(),
                topicGroupOffset.getOffset()
            );
        }
    }

    @Override
    public List<TopicGroupOffset> getQueueMinOffset(Collection<TopicGroupOffset> topicGroupOffsetList) {
        return commitStore.getQueueMinOffset(topicGroupOffsetList);
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
