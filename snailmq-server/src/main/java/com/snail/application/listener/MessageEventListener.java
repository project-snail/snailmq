package com.snail.application.listener;

import com.snail.application.event.AddMessageEvent;
import com.snail.message.Message;
import com.snail.message.MessageExt;
import com.snail.remoting.processor.impl.PullNextMessageOffsetProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.application
 * @Description:
 * @date: 2020/12/22
 */
@Component
public class MessageEventListener {

    @Autowired
    private PullNextMessageOffsetProcessor pullNextMessageOffsetProcessor;

    @Async
    @EventListener
    public void listener(AddMessageEvent event) {
        MessageExt messageExt = event.getMessageExt();
        Message message = messageExt.getMessage();
        pullNextMessageOffsetProcessor.handlerAddMessage(message.getTopic(), messageExt.getQueueId());
    }

}
