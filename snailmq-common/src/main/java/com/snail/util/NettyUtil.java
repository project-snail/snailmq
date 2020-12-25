package com.snail.util;

import io.netty.channel.Channel;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public enum NettyUtil {
    ;

    public static void safeWriteAndFlush(Channel channel, Object object) {
        if (!channel.isWritable()) {
            log.warn("刷回信息时, channel已不可写");
            return;
        }
        channel.writeAndFlush(object);
    }
}
