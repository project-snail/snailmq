package com.snail.remoting.processor.impl;

import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.RegisterCidCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.processor.RemotingCommandProcessor;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.AttributeKey;
import org.springframework.stereotype.Component;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.process.impl
 * @Description:
 * @date: 2020/12/18
 */
@Component
public class RegisterCidProcessor implements RemotingCommandProcessor {

    @Override
    public CommandTypeEnums supportType() {
        return CommandTypeEnums.REGISTER_CID;
    }

    @Override
    public RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand) {
        RegisterCidCommandData registerCidCommandData = RegisterCidCommandData.deserialize(
            remotingCommand.getDataByteBuffer()
        );
        ctx.channel().attr(AttributeKey.valueOf("cid")).set(registerCidCommandData.getCid());
        return remotingCommand;
    }

}
