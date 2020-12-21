package com.snail.remoting.processor.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.snail.consumer.MqService;
import com.snail.consumer.rebalance.RebalanceResult;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.request.FetchRebalanceRequest;
import com.snail.message.RebalanceRequest;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.FetchRebalanceResultCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.processor.RemotingCommandProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.process.impl
 * @Description:
 * @date: 2020/12/18
 */
@Component
public class RebalanceProcessor implements RemotingCommandProcessor {

    @Autowired
    private MqService mqService;

    @Override
    public CommandTypeEnums supportType() {
        return CommandTypeEnums.FETCH_REBALANCE_RESULT;
    }

    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {

        Object cid = ctx.channel().attr(AttributeKey.valueOf("cid")).get();

        if (Objects.isNull(cid)) {
            throw new RuntimeException("暂未设置cid，无法获取offset");
        }

        JsonCommandData<JsonCommandData> jsonCommandData = JsonCommandData.deserialize(
            remotingCommand.getDataByteBuffer()
        );

        List<FetchRebalanceRequest> fetchRebalanceRequestList =  jsonCommandData.getObj(new TypeReference<List<FetchRebalanceRequest>>() {
        });

        List<RebalanceRequest> rebalanceRequestList = fetchRebalanceRequestList.stream()
            .map(topicGroupOffset -> new RebalanceRequest(topicGroupOffset.getTopic(), topicGroupOffset.getGroup()))
            .collect(Collectors.toList());

        List<RebalanceResult> rebalanceResultList = mqService.registerCid(cid.toString(), rebalanceRequestList);

        JsonCommandData<JsonCommandData> commandData = new JsonCommandData<>(rebalanceResultList);

        return new RemotingCommand(CommandTypeEnums.FETCH_TOPIC_GROUP_OFFSET, commandData);
    }
}
