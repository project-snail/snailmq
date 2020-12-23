package com.snail.config;

import com.snail.config.annotation.SnailListener;
import com.snail.config.context.SnailListenerContext;
import com.snail.consumer.ConsumerClientService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/23
 */
public class SnailListenerAnnotationBeanPostProcessor implements BeanPostProcessor {

    private final Set<Class> nonAnnotatedClasses = new HashSet<>();

    @Autowired
    private ConsumerClientService consumerClientService;

    @Autowired
    private ClientProperties clientProperties;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {

        if (!nonAnnotatedClasses.add(bean.getClass())) {
            return bean;
        }

        parseTargetClass(bean, AopUtils.getTargetClass(bean));

        return bean;
    }

    private void parseTargetClass(Object bean, Class targetClass) {

        Map<Method, SnailListener> snailListenerMap = MethodIntrospector.selectMethods(
            targetClass,
            (MethodIntrospector.MetadataLookup<SnailListener>) method -> AnnotationUtils.getAnnotation(
                method,
                SnailListener.class
            )
        );

        snailListenerMap.entrySet().stream()
            .map(entry -> buildContext(bean, entry.getKey(), entry.getValue()))
            .forEach(
                context -> consumerClientService.addMsgListener(
                    context.getTopic(),
                    context.getGroup(),
                    context.getMessageRecordListener()
                )
            );

    }

    private SnailListenerContext buildContext(Object target, Method targetMethod, SnailListener snailListener) {

        if (StringUtils.isBlank(snailListener.topic())) {
            throw new IllegalArgumentException("主题名不得为空");
        }

        SnailListenerContext context = new SnailListenerContext();

        context.setTarget(target);
        context.setTargetMethod(targetMethod);
        context.setTopic(snailListener.topic());
        context.setGroup(snailListener.group());
        context.setSnailListener(snailListener);

        if (StringUtils.isBlank(snailListener.group())) {
            context.setGroup(clientProperties.getDefaultGroup());
        }

        context.init();

        return context;
    }


}
