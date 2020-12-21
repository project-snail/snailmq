package com.snail.remoting.processor.impl;

import com.snail.consumer.MqService;
import com.snail.consumer.TopicGroupOffset;
import com.snail.message.MessageRes;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.*;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.processor.RemotingCommandProcessor;
import com.snail.request.GetMessageRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.processor.impl
 * @Description:
 * @date: 2020/12/18
 */
@Component
public class PullMessageProcessor implements RemotingCommandProcessor {

    @Autowired
    private MqService mqService;

    @Override
    public CommandTypeEnums supportType() {
        return CommandTypeEnums.PULL_MESSAGE;
    }

    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        JsonCommandData<PullMessageCommandData> deserialize = PullMessageCommandData.deserialize(remotingCommand.getDataByteBuffer());

        GetMessageRequest getMessageRequest = deserialize.getObj(GetMessageRequest.class);

//        设置cid
        getMessageRequest.setCid(ctx.channel().attr(AttributeKey.valueOf("cid")).toString());
//        获取请求 带校检
        MessageRes message = mqService.getMessage(getMessageRequest);

        return new RemotingCommand(
            CommandTypeEnums.PUSH_MESSAGE,
            new PullMessageResultCommandData(message)
        );
    }
}
