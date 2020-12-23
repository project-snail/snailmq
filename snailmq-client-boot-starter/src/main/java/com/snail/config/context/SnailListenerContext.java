package com.snail.config.context;

import com.snail.config.annotation.SnailListener;
import com.snail.consumer.listener.PullMessageListener;
import com.snail.message.MessageRecord;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config.context
 * @Description:
 * @date: 2020/12/23
 */
@Slf4j
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SnailListenerContext {

    private Object target;

    private Method targetMethod;

    private SnailListener snailListener;

    private String topic;

    private String group;

    private PullMessageListener messageRecordListener;

    public void init() {
        checkMethod(targetMethod);
        messageRecordListener = messageRecord -> {
            try {
                targetMethod.invoke(target, messageRecord);
            } catch (IllegalAccessException | InvocationTargetException e) {
                log.error("调用监听目标方法失败", e);
            }
        };
    }

    private void checkMethod(Method targetMethod) {
        Class<?>[] parameterTypes = targetMethod.getParameterTypes();
        if (parameterTypes.length != 1 || !MessageRecord.class.equals(parameterTypes[0])) {
            throw new IllegalArgumentException("监听方法入参错误");
        }
    }

}
