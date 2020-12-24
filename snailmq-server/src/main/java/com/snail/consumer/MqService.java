package com.snail.consumer;

import com.snail.message.RebalanceRequest;
import com.snail.consumer.rebalance.RebalanceResult;
import com.snail.request.GetMessageRequest;
import com.snail.message.Message;
import com.snail.message.MessageRes;
import com.snail.request.UpdateOffsetRequest;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/16
 */
public interface MqService {

    MessageRes getMessage(GetMessageRequest request);

    MessageRes getMessage(String topic, int queueId, long offset);

    List<TopicGroupConsumerOffset> getNextMsgOffset(List<TopicGroupOffset> topicGroupList);

    TopicGroupConsumerOffset getNextMsgOffset(TopicGroupOffset topicGroup);

    void addMessage(Message message);

    List<TopicGroupConsumerOffset> getOffset(List<TopicGroupOffset> topicGroupList);

    void updateOffset(UpdateOffsetRequest updateOffsetRequest);

    void updateOffsetBatch(List<UpdateOffsetRequest> updateOffsetRequestList);

    List<TopicGroupOffset> getQueueMinOffset(Collection<TopicGroupOffset> topicGroupOffsetList);

    List<RebalanceResult> registerCid(String cid, List<RebalanceRequest> rebalanceRequestList);

    void removeCid(String cid);

    void registerScheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

}
