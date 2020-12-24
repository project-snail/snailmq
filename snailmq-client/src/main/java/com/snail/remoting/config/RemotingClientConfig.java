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

//    netty心跳最大无响应
    private Integer serverChannelMaxIdleTimeSeconds = 120;

//    可使用的最大线程数
    private Integer maxThreadSize = Integer.MAX_VALUE;

//    netty最大帧大小
    private Integer frameMaxLength = 65535;

//    工作线程数
    private Integer workThreadSize = 4;

//    服务端地址
    private String serverAddr = "";

//    服务端端口
    private Integer serverPort = 8888;

//    链接最大超时时间
    private Integer connectTimeoutMillis = 3000;

//    同步请求最大等待时间
    private Integer syncMaxWaitTimeSeconds = 10 * 60;

}
