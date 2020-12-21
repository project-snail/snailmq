package com.snail.remoting.command.coder;

import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.type.CommandResTypeEnums;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.extern.slf4j.Slf4j;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.coder
 * @Description:
 * @date: 2020/12/17
 */
@Slf4j
public class RemotingCommandNettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) throws Exception {
        try {
            out.writeInt(msg.getSize() + 1);
            out.writeByte(CommandResTypeEnums.ASYNC.ordinal());
            out.writeBytes(msg.serialize());
        } catch (Exception e) {
            log.error("encode 写入异常", e);
            ctx.close();
        }
    }
}
