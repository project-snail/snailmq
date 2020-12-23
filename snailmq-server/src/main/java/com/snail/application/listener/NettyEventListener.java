package com.snail.application.listener;

import com.snail.application.event.NettyEvent;
import com.snail.consumer.MqService;
import com.snail.remoting.netty.event.NettyEventTypeEnums;
import io.netty.channel.Channel;
import io.netty.util.AttributeKey;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.UUID;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.application.listener
 * @Description:
 * @date: 2020/12/23
 */
@Component
public class NettyEventListener {

    @Autowired
    private MqService mqService;

    @Async
    @EventListener
    public void listener(NettyEvent event) {

        Channel channel = event.getChannel();
        NettyEventTypeEnums nettyEventType = event.getNettyEventType();

//        移出关闭的用户
        if (NettyEventTypeEnums.CLOSE.equals(nettyEventType)) {
            Object cid = channel.attr(AttributeKey.valueOf("cid")).get();
            if (cid != null) {
                mqService.removeCid(cid.toString());
            }
        }

//        设置用户cid
        if (NettyEventTypeEnums.CONNECT.equals(nettyEventType)) {
            channel.attr(AttributeKey.valueOf("cid")).set(UUID.randomUUID().toString());
        }


    }

}
