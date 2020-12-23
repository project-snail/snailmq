package com.snail.remoting.netty;

import com.snail.application.event.NettyEvent;
import com.snail.remoting.netty.event.NettyEventTypeEnums;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/23
 */
@Component
public class NettyConnectManageHandler extends ChannelDuplexHandler {

    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.close(ctx, promise);
        applicationEventPublisher.publishEvent(
            new NettyEvent(this, ctx.channel(), NettyEventTypeEnums.CLOSE)
        );
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
        super.disconnect(ctx, promise);
        applicationEventPublisher.publishEvent(
            new NettyEvent(this, ctx.channel(), NettyEventTypeEnums.CLOSE)
        );
    }

}
