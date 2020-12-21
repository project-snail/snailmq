package com.snail.consumer.rebalance.impl;

import com.snail.config.MessageStoreConfig;
import com.snail.message.RebalanceRequest;
import com.snail.consumer.rebalance.RebalanceResult;
import com.snail.consumer.rebalance.RebalanceResultHolder;
import com.snail.consumer.rebalance.RebalanceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.rebalance.impl
 * @Description:
 * @date: 2020/12/18
 */
@Service
public class DefaultRebalanceServiceImpl implements RebalanceService {

    public static final String TOPIC_GROUP_SEPARATOR = "@";

    @Autowired
    private MessageStoreConfig messageStoreConfig;

    private Map<String/* topic@group */, RebalanceResultHolder> rebalanceResultHolderMap = new ConcurrentHashMap<>();

    private Map<String/* cid */, List<RebalanceRequest>> cidTopicGroupMap = new ConcurrentHashMap<>();

    @Override
    public List<RebalanceResult> getRebalanceResult(String cid, List<RebalanceRequest> rebalanceRequestList) {
        return rebalanceRequestList.stream()
            .map(
                rebalanceRequest -> {
                    RebalanceResultHolder rebalanceResultHolder = rebalanceResultHolderMap.get(
                        generateKey(rebalanceRequest.getTopic(), rebalanceRequest.getGroup())
                    );
                    if (rebalanceResultHolder == null) {
                        return null;
                    }
                    return rebalanceResultHolder.getRebalanceResult(cid);
                }
            )
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(Collectors.toList());
    }

    private String generateKey(String topic, String group) {
        return topic + TOPIC_GROUP_SEPARATOR + group;
    }

    @Override
    public void doRebalance(RebalanceRequest rebalanceRequest) {
        RebalanceResultHolder rebalanceResultHolder = rebalanceResultHolderMap.computeIfAbsent(
            generateKey(rebalanceRequest.getTopic(), rebalanceRequest.getGroup()),
            key -> new RebalanceResultHolder(
                rebalanceRequest.getTopic(),
                rebalanceRequest.getGroup(),
                this.messageStoreConfig.getQueueSize()
            )
        );
        rebalanceResultHolder.rebalance();
    }

    @Override
    public void addCid(String cid, List<RebalanceRequest> rebalanceRequestList) {
        updateCid(cid, true, rebalanceRequestList);
        this.cidTopicGroupMap.put(cid, rebalanceRequestList);
    }

    @Override
    public void removeCid(String cid) {
        List<RebalanceRequest> rebalanceRequestList = this.cidTopicGroupMap.getOrDefault(cid, Collections.emptyList());
        updateCid(cid, false, rebalanceRequestList);
        this.cidTopicGroupMap.remove(cid);
    }

    private void updateCid(String cid, boolean isAdd, List<RebalanceRequest> rebalanceRequestList) {
        for (RebalanceRequest rebalanceRequest : rebalanceRequestList) {
            RebalanceResultHolder rebalanceResultHolder = rebalanceResultHolderMap.computeIfAbsent(
                generateKey(rebalanceRequest.getTopic(), rebalanceRequest.getGroup()),
                key -> new RebalanceResultHolder(
                    rebalanceRequest.getTopic(),
                    rebalanceRequest.getGroup(),
                    this.messageStoreConfig.getQueueSize()
                )
            );
            rebalanceResultHolder.updateCid(cid, isAdd);
        }
    }

    @Override
    public boolean checkOwner(String cid, int version, String topic, String group, int queueId) {
        return Optional.ofNullable(this.rebalanceResultHolderMap.get(generateKey(topic, group)))
            .map(rebalanceResultHolder -> rebalanceResultHolder.isOwner(cid, version, queueId))
            .orElse(Boolean.FALSE);
    }
}
