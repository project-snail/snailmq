package com.snail.consumer.rebalance;

import lombok.Data;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.rebalance
 * @Description:
 * @date: 2020/12/18
 */
@Data
public class RebalanceResultHolder {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final String topic;

    private final String group;

    private final int queueSize;

    private final AtomicInteger rebalanceVersion;

    private Set<String> cidList;

    private List<RebalanceResult> rebalanceResultList;

    private Map<String, Set<Integer>> cidQueueMap;

    public RebalanceResultHolder(String topic, String group, int queueSize) {
        this.topic = topic;
        this.group = group;
        this.queueSize = queueSize;
        this.rebalanceVersion = new AtomicInteger();
        this.cidList = new HashSet<>();
        this.rebalanceResultList = Collections.emptyList();
        this.cidQueueMap = Collections.emptyMap();
    }

    public List<RebalanceResult> getRebalanceResult(String cid) {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        try {
            readLock.lock();
            Set<Integer> queueIds = cidQueueMap.getOrDefault(cid, Collections.emptySet());
            return queueIds.stream()
                .map(queueId -> buildRebalanceResult(cid, queueId))
                .collect(Collectors.toList());
        } finally {
            readLock.unlock();
        }
    }

    private RebalanceResult buildRebalanceResult(String cid, int queueId) {

        RebalanceResult result = new RebalanceResult();

//        result.setCid(cid);
        result.setQueueId(queueId);
        result.setTopic(topic);
        result.setGroup(group);
        result.setVersion(rebalanceVersion.get());

        return result;

    }

    public void updateCid(String cid, boolean isAdd) {

        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        try {
            writeLock.lock();

            boolean addRes = isAdd ? cidList.add(cid) : cidList.remove(cid);

            if (!addRes) {
                return;
            }

            doRebalance();

        } finally {
            writeLock.unlock();
        }

    }

    public void rebalance() {
        ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
        try {
            writeLock.lock();

            doRebalance();

        } finally {
            writeLock.unlock();
        }
    }

    private void doRebalance() {

        ArrayList<RebalanceResult> rebalanceResultList = new ArrayList<>(queueSize);
        HashMap<String, Set<Integer>> cidQueueMap = new HashMap<>();

        for (int i = 0; i < queueSize && !cidList.isEmpty(); ) {
            Iterator<String> iterator = this.cidList.iterator();
            while (iterator.hasNext() && i < queueSize) {
                String curCid = iterator.next();
                RebalanceResult rebalanceResult = buildRebalanceResult(curCid, i);
                rebalanceResultList.add(rebalanceResult);
                Set<Integer> queueSet = cidQueueMap.computeIfAbsent(curCid, key -> new HashSet<>());
                queueSet.add(i);
                i++;
            }
        }

        this.rebalanceResultList = rebalanceResultList;
        this.cidQueueMap = cidQueueMap;
        this.rebalanceVersion.incrementAndGet();

    }

    public boolean isOwner(String cid, int version, int queueId) {
        ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
        try {
            readLock.lock();
            return version == rebalanceVersion.get()
                && this.cidQueueMap.getOrDefault(cid, Collections.emptySet()).contains(queueId);
        } finally {
            readLock.unlock();
        }
    }

}
