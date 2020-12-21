package com.snail.remoting.processor;

import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.type.CommandTypeEnums;
import io.netty.channel.ChannelHandlerContext;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting
 * @Description:
 * @date: 2020/12/18
 */
public interface RemotingCommandProcessor {

    CommandTypeEnums supportType();

    RemotingCommand process(ChannelHandlerContext ctx, RemotingCommand remotingCommand);

}
