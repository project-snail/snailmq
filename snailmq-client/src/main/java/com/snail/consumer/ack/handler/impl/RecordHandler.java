package com.snail.consumer.ack.handler.impl;

import com.snail.consumer.ack.handler.AckAbstractHandler;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack.handler.impl
 * @Description:
 * @date: 2020/12/24
 */
public class RecordHandler extends AckAbstractHandler {
    @Override
    public void ack(long offset) {
        super.ack(offset);
    }
}
