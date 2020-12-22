package com.snail.remoting.config;

import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.config
 * @Description:
 * @date: 2020/12/17
 */
@Data
public class RemotingClientConfig {

    private Integer serverChannelMaxIdleTimeSeconds = 120;

    private Integer maxListenerSize = Integer.MAX_VALUE;

    private Integer frameMaxLength = 65535;

    private Integer workThreadSize = 4;

    private String serverAddr = "127.0.0.1";

    private Integer serverPort = 8888;

    private Integer connectTimeoutMillis = 3000;

    private Integer syncMaxWaitTimeSeconds = 10 * 60;

}
