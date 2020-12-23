package com.snail.config;

import com.snail.consumer.ConsumerClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/23
 */
public class SnailListenerRunner implements ApplicationRunner {

    @Autowired
    private ConsumerClientService consumerClientService;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        consumerClientService.startListener();
    }

}
