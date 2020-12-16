package com.snail.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

import java.io.File;


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
@ConfigurationProperties(prefix = "message.store")
public class MessageStoreConfig {

    public final static int MESSAGE_MAGIC_CODE = 0xababf;

    public static final String TOPIC_GROUP_SEPARATOR = "@";

    private Integer commitLogFileSize = 1024 * 1024 * 1024;

    private Integer maxTopicLength = 128;

    private Integer maxQueueItemSize = 10000;

    private Integer persistenceConsumerGroupOffsetInterval = 1000 * 5;

    private Integer queueSize = 6;

    private String baseDirPath = "store/";

    public final String commitLogDirPrefix = "commitLog" + File.separator;

    public final String queueDirPath = "queue" + File.separator;

    public final String groupOffsetConfigPath = "config" + File.separator + "groupOffsetConfig.json";

}
