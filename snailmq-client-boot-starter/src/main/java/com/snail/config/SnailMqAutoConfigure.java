package com.snail.config;

import com.snail.config.template.SnailMqTemplate;
import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.impl.ConsumerClientServiceImpl;
import com.snail.remoting.config.RemotingClientConfig;
import org.springframework.boot.autoconfigure.AutoConfigureOrder;
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
@Import({
    SnailListenerAnnotationBeanPostProcessor.class,
    SnailListenerRunner.class
})
public class SnailMqAutoConfigure {

    @Bean
    public ConsumerClientService consumerClientService(ClientProperties properties) {

        RemotingClientConfig config = new RemotingClientConfig();
        config.setServerChannelMaxIdleTimeSeconds(properties.getServerChannelMaxIdleTimeSeconds());
        config.setMaxThreadSize(properties.getMaxThreadSize());
        config.setFrameMaxLength(properties.getFrameMaxLength());
        config.setWorkThreadSize(properties.getWorkThreadSize());
        config.setServerAddr(properties.getServerAddr());
        config.setServerPort(properties.getServerPort());
        config.setConnectTimeoutMillis(properties.getConnectTimeoutMillis());
        config.setSyncMaxWaitTimeSeconds(properties.getSyncMaxWaitTimeSeconds());

        return new ConsumerClientServiceImpl(config);

    }

    @Bean
    @ConditionalOnMissingBean(SnailMqTemplate.class)
    public SnailMqTemplate snailMqTemplate() {
        return new SnailMqTemplate();
    }

}
