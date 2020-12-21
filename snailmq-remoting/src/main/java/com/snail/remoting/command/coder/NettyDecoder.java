package com.snail.remoting.command.coder;

import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.type.CommandResTypeEnums;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

@Slf4j
public class NettyDecoder extends LengthFieldBasedFrameDecoder {

    public NettyDecoder(int frameMaxLength) {
        super(frameMaxLength, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }

            ByteBuffer byteBuffer = frame.nioBuffer();

            byte resType = byteBuffer.get();

            if (resType > CommandResTypeEnums.values().length) {
                throw new RuntimeException("消息类型异常");
            }

            CommandResTypeEnums commandResType = CommandResTypeEnums.values()[resType];

//            序列化RemotingCommand
            switch (commandResType) {
                case SYNC:
                    return SyncRemotingCommand.deserialize(byteBuffer);
                case ASYNC:
                    return RemotingCommand.deserialize(byteBuffer);
            }

        } catch (Exception e) {
            log.error("decode 读取异常", e);
            ctx.close();
        } finally {
            if (null != frame) {
                frame.release();
            }
        }

        return null;
    }
}
