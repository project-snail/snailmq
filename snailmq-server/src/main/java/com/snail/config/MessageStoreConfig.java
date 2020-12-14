package com.snail.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;


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

    private Integer commitLogFileSize = 1024 * 1024 * 1024;

    private Integer maxTopicLength = 128;

    private Integer maxQueueItemSize = 10000;

}
