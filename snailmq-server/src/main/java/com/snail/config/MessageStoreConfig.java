package com.snail.config;

import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.commit.FlushDiskHandler;
import com.snail.commit.impl.AsyncFlushDiskHandler;
import com.snail.commit.impl.SyncFlushDiskHandler;
import com.snail.type.FlushDiskTypeEnums;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/13
 */
@Data
@Component
@Configuration
@ConfigurationProperties(prefix = "snail.message.store")
public class MessageStoreConfig {

    public final static int MESSAGE_MAGIC_CODE = 0xababf;

    public static final String TOPIC_QUEUE_SEPARATOR = "@";

    private Integer commitLogFileSize = 1024 * 1024 * 1024;

    private Integer maxTopicLength = 128;

    private Integer maxQueueItemSize = 10000;

    private Integer persistenceConsumerGroupOffsetInterval = 1000 * 5;

    private Integer queueSize = 6;

    private String baseDirPath = "store/";

    public final String commitLogDirPrefix = "commitLog" + File.separator;

    public final String queueDirPath = "queue" + File.separator;

    public final String groupOffsetConfigPath = "config" + File.separator + "groupOffsetConfig.json";

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new NamedThreadFactory("SnailMqScheduledExecutor", false)
    );

    private FlushDiskTypeEnums flushDiskType = FlushDiskTypeEnums.ASYNC_FLUSH;

    //    异步刷盘间隔时间
    private Integer flushDishInterval = 500;

    @Bean
    public FlushDiskHandler flushDiskHandler() {
        if (this.flushDiskType == FlushDiskTypeEnums.ASYNC_FLUSH) {
            return new AsyncFlushDiskHandler(scheduledExecutorService, flushDishInterval);
        } else {
            return new SyncFlushDiskHandler();
        }
    }


}
