package com.snail.remoting.holder;

import com.snail.request.PullNextMessageOffsetRequest;
import io.netty.channel.Channel;
import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.holder
 * @Description:
 * @date: 2020/12/22
 */
@Data
public class PullNextMessageOffsetRequestHolder {

    private PullNextMessageOffsetRequest request;

    private long time = System.currentTimeMillis();

    private volatile boolean isNotify = false;

    private Channel channel;

}
