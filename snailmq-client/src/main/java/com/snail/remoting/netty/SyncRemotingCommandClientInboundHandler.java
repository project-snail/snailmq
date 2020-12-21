package com.snail.remoting.netty;

import com.snail.remoting.command.SyncRemotingCommand;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/18
 */
public class SyncRemotingCommandClientInboundHandler extends SimpleChannelInboundHandler<SyncRemotingCommand> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncRemotingCommand msg) throws Exception {
        msg.pushRes();
    }

}
