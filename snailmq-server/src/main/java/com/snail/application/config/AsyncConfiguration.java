package com.snail.application.config;

import cn.hutool.core.thread.NamedThreadFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.application.config
 * @Description:
 * @date: 2020/12/22
 */
@EnableAsync
@Configuration
public class AsyncConfiguration implements AsyncConfigurer {
    @Override
    public Executor getAsyncExecutor() {
        return new ThreadPoolExecutor(
            8, 16,
            300, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(64),
            new NamedThreadFactory("async_annotation_pool", false),
            new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
