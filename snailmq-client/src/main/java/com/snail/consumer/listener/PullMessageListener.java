package com.snail.consumer.listener;

import com.snail.message.MessageRecord;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.listener
 * @Description:
 * @date: 2020/12/17
 */
public interface PullMessageListener {
    void listener(MessageRecord messageRecord);
}
