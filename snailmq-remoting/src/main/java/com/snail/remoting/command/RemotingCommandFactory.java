package com.snail.remoting.command;

import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.Message;
import com.snail.remoting.command.data.*;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.request.FetchRebalanceRequest;
import com.snail.request.GetMessageRequest;
import com.snail.request.UpdateOffsetRequest;

import java.util.List;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command
 * @Description:
 * @date: 2020/12/18
 */
public enum RemotingCommandFactory {
    ;

    public static RemotingCommand registerCid(String cid) {
        return new RemotingCommand(
            CommandTypeEnums.REGISTER_CID,
            new RegisterCidCommandData(cid)
        );
    }

    public static RemotingCommand fetchRebalanceResult(List<FetchRebalanceRequest> fetchRebalanceRequests) {
        return new RemotingCommand(
            CommandTypeEnums.FETCH_REBALANCE_RESULT,
            new FetchRebalanceResultCommandData(fetchRebalanceRequests)
        );
    }

    public static RemotingCommand fetchTopicGroupOffset(List<TopicGroupOffset> topicGroupOffsets) {
        return new RemotingCommand(
            CommandTypeEnums.FETCH_TOPIC_GROUP_OFFSET,
            new FetchTopicGroupOffsetCommandData(topicGroupOffsets)
        );
    }

    public static RemotingCommand fetchTopicGroupOffsetResult(List<TopicGroupConsumerOffset> topicGroupConsumerOffsets) {
        return new RemotingCommand(
            CommandTypeEnums.FETCH_TOPIC_GROUP_OFFSET,
            new FetchTopicGroupOffsetConsumerCommandData(topicGroupConsumerOffsets)
        );
    }

    public static RemotingCommand pullMessage(GetMessageRequest getMessageRequest) {
        return new RemotingCommand(
            CommandTypeEnums.PULL_MESSAGE,
            new PullMessageCommandData(getMessageRequest)
        );
    }

    public static RemotingCommand pushMessage(Message message) {
        return new RemotingCommand(
            CommandTypeEnums.PUSH_MESSAGE,
            new PushMessageCommandData(message)
        );
    }

    public static RemotingCommand updateOffset(UpdateOffsetRequest request) {
        return new RemotingCommand(
            CommandTypeEnums.UPDATE_OFFSET,
            new UpdateOffsetCommandData(request)
        );
    }


}
