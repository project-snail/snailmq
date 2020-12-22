package com.snail.remoting.processor.impl;

import com.snail.consumer.MqService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.holder.PullNextMessageOffsetRequestHolder;
import com.snail.remoting.processor.RemotingCommandProcessor;
import com.snail.request.PullNextMessageOffsetRequest;
import com.snail.store.IntStoreItem;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.processor.impl
 * @Description:
 * @date: 2020/12/22
 */
@Component
public class PullNextMessageOffsetProcessor implements RemotingCommandProcessor {

    @Autowired
    private MqService mqService;

    private static final String MAP_SEPARATOR = "@";

    //    TODO 清理
    private static final Map<String, PullNextMessageOffsetRequestHolder> requestHolderMap = new HashMap<>();

    @Override
    public CommandTypeEnums supportType() {
        return CommandTypeEnums.PULL_NEXT_MESSAGE_OFFSET;
    }

    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        JsonCommandData<JsonCommandData> deserialize = JsonCommandData.deserialize(remotingCommand.getDataByteBuffer());
        PullNextMessageOffsetRequest request = deserialize.getObj(PullNextMessageOffsetRequest.class);

        TopicGroupConsumerOffset nextMsgOffset = findNextOffset(request);

        if (nextMsgOffset == null || nextMsgOffset.getNextMsgOffset() == -1) {
            PullNextMessageOffsetRequestHolder requestHolder = new PullNextMessageOffsetRequestHolder();
            requestHolder.setChannel(ctx.channel());
            requestHolder.setRequest(request);
            requestHolder.setTime(System.currentTimeMillis());
            requestHolderMap.put(
                buildKey(request.getTopic(), request.getQueueId()),
                requestHolder
            );
            return null;
        }

        return new RemotingCommand(CommandTypeEnums.PULL_NEXT_MESSAGE_OFFSET, new JsonCommandData(nextMsgOffset));

    }

    private TopicGroupConsumerOffset findNextOffset(PullNextMessageOffsetRequest request) {
        TopicGroupOffset topicGroupOffset = new TopicGroupOffset();
        topicGroupOffset.setOffset(request.getOffset());
        topicGroupOffset.setQueueId(request.getQueueId());
        topicGroupOffset.setGroup(request.getGroup());
        topicGroupOffset.setTopic(request.getTopic());
        return mqService.getNextMsgOffset(topicGroupOffset);
    }

    public void handlerAddMessage(String topic, long queueId) {

        String pullHolderKey = buildKey(topic, queueId);

        PullNextMessageOffsetRequestHolder requestHolder = requestHolderMap.get(pullHolderKey);
        if (requestHolder == null) {
            return;
        }

        synchronized (requestHolder) {
//            已经通知过了
            if (requestHolder.isNotify()) {
                return;
            }
            Channel channel = requestHolder.getChannel();
            if (!channel.isWritable()) {
                return;
            }
            PullNextMessageOffsetRequest request = requestHolder.getRequest();
//            查找下一个偏移量
            TopicGroupConsumerOffset nextMsgOffset = findNextOffset(request);
            if (nextMsgOffset != null && nextMsgOffset.getNextMsgOffset() != -1)
                channel.writeAndFlush(
                    new SyncRemotingCommand(
                        new RemotingCommand(
                            CommandTypeEnums.PULL_NEXT_MESSAGE_OFFSET,
                            new JsonCommandData(nextMsgOffset)
                        ),
                        new IntStoreItem(request.getSyncCode())
                    )
                );
            requestHolderMap.remove(pullHolderKey);
            requestHolder.setNotify(true);
        }
    }

    private String buildKey(String topic, long queueId) {
        return topic + MAP_SEPARATOR + queueId;
    }


}
