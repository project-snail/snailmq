package com.snail.config;

import com.snail.config.template.SnailMqTemplate;
import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.config.ConsumerClientConfig;
import com.snail.consumer.impl.ConsumerClientServiceImpl;
import com.snail.remoting.config.RemotingClientConfig;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.Ordered;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.config
 * @Description:
 * @date: 2020/12/23
 */
@Configuration
@EnableConfigurationProperties(ClientProperties.class)
@AutoConfigureOrder(Ordered.LOWEST_PRECEDENCE)
@ConditionalOnBean(SnailListenerRunner.class)
@Import({
    SnailListenerAnnotationBeanPostProcessor.class
})
public class SnailMqAutoConfigure {

    @Bean
    public ConsumerClientService consumerClientService(ClientProperties properties) {

        RemotingClientConfig remotingClientConfig = new RemotingClientConfig();
        remotingClientConfig.setServerChannelMaxIdleTimeSeconds(properties.getServerChannelMaxIdleTimeSeconds());
        remotingClientConfig.setMaxThreadSize(properties.getMaxThreadSize());
        remotingClientConfig.setFrameMaxLength(properties.getFrameMaxLength());
        remotingClientConfig.setWorkThreadSize(properties.getWorkThreadSize());
        remotingClientConfig.setServerAddr(properties.getServerAddr());
        remotingClientConfig.setServerPort(properties.getServerPort());
        remotingClientConfig.setConnectTimeoutMillis(properties.getConnectTimeoutMillis());
        remotingClientConfig.setSyncMaxWaitTimeSeconds(properties.getSyncMaxWaitTimeSeconds());

        ConsumerClientConfig consumerClientConfig = new ConsumerClientConfig();
        consumerClientConfig.setAckCount(properties.getAckCount());
        consumerClientConfig.setAckMode(properties.getAckMode());
        consumerClientConfig.setAckTimeSeconds(properties.getAckTimeSeconds());

        return new ConsumerClientServiceImpl(remotingClientConfig, consumerClientConfig);

    }

    @Bean
    @ConditionalOnMissingBean(SnailMqTemplate.class)
    public SnailMqTemplate snailMqTemplate() {
        return new SnailMqTemplate();
    }

}
