package com.snail.application.event;

import com.snail.message.MessageExt;
import org.springframework.context.ApplicationEvent;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.application.event
 * @Description:
 * @date: 2020/12/22
 */
public class AddMessageEvent extends ApplicationEvent {

    private final MessageExt messageExt;

    /**
     * Create a new {@code ApplicationEvent}.
     *
     * @param source the object on which the event initially occurred or with
     *               which the event is associated (never {@code null})
     * @param messageExt
     */
    public AddMessageEvent(Object source, MessageExt messageExt) {
        super(source);
        this.messageExt = messageExt;
    }

    public MessageExt getMessageExt() {
        return messageExt;
    }
}
