package com.snail.remoting.netty;

import com.snail.exception.SnailBaseException;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.store.IntStoreItem;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/18
 */
@Slf4j
@Component
@ChannelHandler.Sharable
public class SyncRemotingCommandInboundHandler extends SimpleChannelInboundHandler<SyncRemotingCommand> {

    @Autowired
    private RemotingCommandInboundHandler remotingCommandInboundHandler;

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, SyncRemotingCommand msg) throws Exception {
        RemotingCommand remotingCommand;
        try {
            remotingCommand = remotingCommandInboundHandler.handler(ctx, msg.getRes());
            if (remotingCommand != null) {
                ctx.writeAndFlush(new SyncRemotingCommand(remotingCommand, new IntStoreItem(msg.getSyncCode())));
            }
        } catch (Exception e) {
            log.error("netty处理请求异常", e);
            if (e instanceof SnailBaseException) {
                remotingCommand = CommandExceptionStateEnums.buildExRemotingCommand(((SnailBaseException) e).getExceptionState());
            } else {
                remotingCommand = CommandExceptionStateEnums.buildExRemotingCommand(CommandExceptionStateEnums.ERROR);
            }
            ctx.writeAndFlush(new SyncRemotingCommand(remotingCommand, new IntStoreItem(msg.getSyncCode())));
        }
    }

}
