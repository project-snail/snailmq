package com.snail.remoting.netty;

import com.snail.exception.SnailBaseException;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.RemotingCommandData;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.processor.RemotingCommandProcessor;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

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
public class RemotingCommandInboundHandler extends SimpleChannelInboundHandler<RemotingCommand> {

    private static Map<CommandTypeEnums, RemotingCommandProcessor> processorMap;

    public RemotingCommandInboundHandler(List<RemotingCommandProcessor> processorList) {
        processorMap = processorList.stream()
            .collect(Collectors.toMap(RemotingCommandProcessor::supportType, Function.identity()));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand msg) throws Exception {
        RemotingCommand remotingCommand = handler(ctx, msg);
        if (remotingCommand != null) {
            ctx.writeAndFlush(remotingCommand);
        }
    }

    public RemotingCommand handler(ChannelHandlerContext ctx, RemotingCommand msg) {
        RemotingCommandProcessor processor = processorMap.get(msg.getCommandType());
        if (processor == null) {
            return null;
        }
        return processor.process(ctx, msg);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("netty处理请求异常", cause);
        CommandExceptionStateEnums exceptionState = CommandExceptionStateEnums.ERROR;
        if (cause instanceof SnailBaseException) {
            exceptionState = ((SnailBaseException) cause).getExceptionState();
        }
        RemotingCommand remotingCommand = new RemotingCommand(CommandTypeEnums.ERROR, RemotingCommandData.OK);
        remotingCommand.setExceptionState(exceptionState);
        Channel channel = ctx.channel();
        if (channel.isWritable()) {
            channel.writeAndFlush(remotingCommand);
        }
    }
}
