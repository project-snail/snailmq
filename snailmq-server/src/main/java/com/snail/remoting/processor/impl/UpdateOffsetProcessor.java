package com.snail.remoting.processor.impl;

import com.snail.consumer.MqService;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.remoting.command.data.PushMessageCommandData;
import com.snail.remoting.command.data.RemotingCommandData;
import com.snail.remoting.command.data.UpdateOffsetCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.processor.RemotingCommandProcessor;
import com.snail.request.UpdateOffsetRequest;
import io.netty.channel.ChannelHandlerContext;
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
public class UpdateOffsetProcessor implements RemotingCommandProcessor {

    @Autowired
    private MqService mqService;

    @Override
    public CommandTypeEnums supportType() {
        return CommandTypeEnums.UPDATE_OFFSET;
    }

    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        JsonCommandData<JsonCommandData> deserialize = UpdateOffsetCommandData.deserialize(remotingCommand.getDataByteBuffer());
        UpdateOffsetRequest updateOffsetRequest = deserialize.getObj(UpdateOffsetRequest.class);

        mqService.updateOffset(updateOffsetRequest);

        return new RemotingCommand(CommandTypeEnums.OK, RemotingCommandData.OK);
    }
}
