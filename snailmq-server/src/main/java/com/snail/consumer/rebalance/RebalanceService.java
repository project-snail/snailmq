package com.snail.consumer.rebalance;

import com.snail.message.RebalanceRequest;

import java.util.List;

/**
 * @version V1.0
 * @author: csz
 * @Title 分配queue服务
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/17
 */
public interface RebalanceService {

    /**
     * 根据唯一id 获取分配到的queue
     *
     * @param cid                  唯一id
     * @param rebalanceRequestList 需要获取的主题和组
     * @return 分配结果
     */
    List<RebalanceResult> getRebalanceResult(String cid, List<RebalanceRequest> rebalanceRequestList);

    /**
     * 将topic和group的集合重新分配
     *
     * @param rebalanceRequest topic和group
     */
    void doRebalance(RebalanceRequest rebalanceRequest);

    /**
     * 添加cid和所需要拉取的topic和group
     *
     * @param cid                  唯一id
     * @param rebalanceRequestList topic和group集合
     */
    void addCid(String cid, List<RebalanceRequest> rebalanceRequestList);

    /**
     * 有机器下线了
     *
     * @param cid 唯一id
     */
    void removeCid(String cid);

    /**
     * 检查该机cid是否拥有这个queue的所有权
     *
     * @return 是否拥有
     */
    boolean checkOwner(String cid, int version, String topic, String group, int queueId);


}
