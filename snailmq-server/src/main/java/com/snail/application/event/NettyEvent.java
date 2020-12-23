package com.snail.application.event;

import com.snail.remoting.netty.event.NettyEventTypeEnums;
import io.netty.channel.Channel;
import org.springframework.context.ApplicationEvent;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.application.event
 * @Description:
 * @date: 2020/12/23
 */
public class NettyEvent extends ApplicationEvent {

    private final Channel channel;

    private final NettyEventTypeEnums nettyEventType;

    public NettyEvent(Object source, Channel channel, NettyEventTypeEnums nettyEventType) {
        super(source);
        this.channel = channel;
        this.nettyEventType = nettyEventType;
    }

    public Channel getChannel() {
        return channel;
    }

    public NettyEventTypeEnums getNettyEventType() {
        return nettyEventType;
    }
}

