package com.snail.config;

import com.snail.consumer.ack.AckModeEnums;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/23
 */
@Data
@ConfigurationProperties(prefix = "snail.mq")
public class ClientProperties {

    private Integer serverChannelMaxIdleTimeSeconds = 120;

    private Integer maxThreadSize = Integer.MAX_VALUE;

    private Integer frameMaxLength = 65535;

    private Integer workThreadSize = 4;

    private String serverAddr = "";

    private Integer serverPort = 8888;

    private Integer connectTimeoutMillis = 3000;

    private Integer syncMaxWaitTimeSeconds = 10 * 60;

    private String defaultGroup = "foo";

    private Integer ackTimeSeconds = 10 * 60;

    private Integer ackCount = 1;

    private AckModeEnums ackMode = AckModeEnums.RECORD;

}
