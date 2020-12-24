package com.snail.config.annotation;

import com.snail.consumer.ack.AckModeEnums;

import java.lang.annotation.*;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config.annotation
 * @Description:
 * @date: 2020/12/23
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface SnailListener {

    //  主题
    String topic();

    //  组名 为空的话 使用默认组名
    String group() default "";

    //  ack模式
    AckModeEnums ackMode() default AckModeEnums.RECORD;

}
