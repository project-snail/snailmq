package com.snail.remoting.netty;

import com.snail.application.event.NettyEvent;
import com.snail.remoting.netty.event.NettyEventTypeEnums;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.net.SocketAddress;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/23
 */
@Component
@ChannelHandler.Sharable
public class NettyConnectManageHandler extends ChannelDuplexHandler {

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        applicationEventPublisher.publishEvent(
            new NettyEvent(this, ctx.channel(), NettyEventTypeEnums.CONNECT)
        );
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        applicationEventPublisher.publishEvent(
            new NettyEvent(this, ctx.channel(), NettyEventTypeEnums.CLOSE)
        );
    }

}
