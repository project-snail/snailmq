package com.snail.consumer.ack;

import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.ack.handler.AckHandler;
import com.snail.consumer.ack.handler.impl.CountHandler;
import com.snail.consumer.ack.handler.impl.CountTimeHandler;
import com.snail.consumer.ack.handler.impl.RecordHandler;
import com.snail.consumer.ack.handler.impl.TimeHandler;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.ack
 * @Description:
 * @date: 2020/12/24
 */
public enum AckModeEnums {

    //    每条消息
    RECORD(RecordHandler.class),

    //    定时
    TIME(TimeHandler.class),

    //    累计
    COUNT(CountHandler.class),

    //    累计加定时
    COUNT_TIME(CountTimeHandler.class),

    //    手动
    MANUAL(RecordHandler.class);

    private final Class<? extends AckHandler> ackHandlerClass;

    AckModeEnums(Class<? extends AckHandler> ackHandlerClass) {
        this.ackHandlerClass = ackHandlerClass;
    }

    public AckHandler createAckHandler(ConsumerClientService service, String topic, String group, int queueId) {
        try {
            Constructor<? extends AckHandler> constructor = this.ackHandlerClass.getConstructor();
            AckHandler ackHandler = constructor.newInstance();
            ackHandler.init(service, topic, group, queueId);
            return ackHandler;
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("创建ack处理器失败", e);
        }
    }

}
