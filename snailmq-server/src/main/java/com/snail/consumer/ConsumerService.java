package com.snail.consumer;

import com.snail.message.Message;
import com.snail.message.MessageRes;

import java.util.Collection;
import java.util.List;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/16
 */
public interface ConsumerService {

    MessageRes getMessage(String topic, int queueId, long offset);

    List<TopicGroupOffset> getNextMsgOffset(List<TopicGroupOffset> topicGroupList);

    void addMessage(Message message);

    List<TopicGroupOffset> getOffset(List<TopicGroupOffset> topicGroupList);

    void updateOffset(TopicGroupOffset topicGroupOffset);

    void updateOffsetBatch(List<TopicGroupOffset> topicGroupOffsetList);

    List<TopicGroupOffset> getQueueMinOffset(Collection<TopicGroupOffset> topicGroupOffsetList);

}
