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
 * @date: 2020/12/17
 */
@Data
@Component
@Configuration
@ConfigurationProperties(prefix = "snail.remoting.server")
public class RemotingServerConfig {

    private Boolean useEpoll = Boolean.FALSE;

    private Integer selectorThreadSize = 3;

    private Integer frameMaxLength = 65535;

    private Integer workThreadSize = 8;

    private Integer listenPort = 8888;

    private Integer serverChannelMaxIdleTimeSeconds = 120;

}
