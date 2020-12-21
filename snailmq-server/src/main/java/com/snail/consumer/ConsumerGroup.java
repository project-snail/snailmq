package com.snail.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snail.config.MessageStoreConfig;
import com.snail.util.FileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/15
 */
@Slf4j
@Service
public class ConsumerGroup implements InitializingBean {

    private Map<String/* group */, Map<String/* topic */, Map<Integer/* queueId */, AtomicLong /* offset */>>> consumerOffsetMap;

    @Autowired
    private MessageStoreConfig messageStoreConfig;

    @Override
    public void afterPropertiesSet() throws Exception {

        File file = new File(messageStoreConfig.getBaseDirPath() + messageStoreConfig.getGroupOffsetConfigPath());

        if (!file.exists()) {

            if (!file.getParentFile().exists()) {
                boolean mkdirs = file.getParentFile().mkdirs();
                log.info("创建ConsumerGroup配置文件夹" + (mkdirs ? "OK" : "FAILED"));
            }

            boolean newFile = file.createNewFile();
            log.info("创建ConsumerGroup配置文件" + (newFile ? "OK" : "FAILED"));

        }

        String configStr;

        try {
            configStr = FileUtil.readFileWithBackupWhenError(file.getPath());
        } catch (IOException e) {
            log.error("读取ConsumerGroup配置文件失败", e);
            throw new RuntimeException("读取ConsumerGroup配置文件失败");
        }

        if (StringUtils.isBlank(configStr)) {
            this.consumerOffsetMap = new ConcurrentHashMap<>();
        } else {
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                this.consumerOffsetMap = objectMapper.readValue(
                    configStr,
                    new TypeReference<Map<String, Map<String, Map<Integer, AtomicLong>>>>() {
                    }
                );
            } catch (IOException e) {
                log.error("反序列配置文件失败", e);
                throw new RuntimeException("读取ConsumerGroup配置文件失败");
            }
        }

        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));

    }

    public synchronized void persistence() {

        ObjectMapper objectMapper = new ObjectMapper();
        String persistenceContent;
        try {
            persistenceContent = objectMapper.writeValueAsString(this.consumerOffsetMap);
        } catch (JsonProcessingException e) {
            log.error("持久化消费者偏移量失败", e);
            return;
        }

        String configPath = messageStoreConfig.getBaseDirPath() + messageStoreConfig.getGroupOffsetConfigPath();

        FileUtil.writeToFileWithBackup(persistenceContent, configPath);

    }

    public void updateOffset(String topic, String group, int queueId, Long offset) {
        Map<String, Map<Integer, AtomicLong>> topicQueueMap = consumerOffsetMap.computeIfAbsent(
            group,
            k -> new ConcurrentHashMap<>()
        );
        Map<Integer, AtomicLong> queueIdOffsetMap = topicQueueMap.computeIfAbsent(
            topic,
            k -> new ConcurrentHashMap<>()
        );
        AtomicLong offsetAtomic = queueIdOffsetMap.computeIfAbsent(queueId, k -> new AtomicLong(offset));

        offsetAtomic.set(offset);
    }

    public List<TopicGroupOffset> getOffset(List<TopicGroupOffset> topicGroupList) {
        return topicGroupList.stream()
            .map(
                topicGroupOffset -> {
                    Long offset = Optional.ofNullable(this.consumerOffsetMap.get(topicGroupOffset.getGroup()))
                        .map(map -> map.get(topicGroupOffset.getTopic()))
                        .map(map -> map.get(topicGroupOffset.getQueueId()))
                        .map(AtomicLong::get).orElse(-1L);
                    return new TopicGroupOffset(
                        topicGroupOffset.getTopic(),
                        topicGroupOffset.getGroup(),
                        topicGroupOffset.getQueueId(),
                        offset
                    );
                }
            )
            .collect(Collectors.toList());
    }

    private void shutdown() {
        this.persistence();
    }

}
