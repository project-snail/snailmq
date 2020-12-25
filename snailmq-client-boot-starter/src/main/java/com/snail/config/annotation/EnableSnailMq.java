package com.snail.config.annotation;

import com.snail.config.SnailListenerRunner;
import com.snail.config.SnailMqAutoConfigure;
import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/23
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SnailListenerRunner.class)
public @interface EnableSnailMq {
}
