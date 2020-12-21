package com.snail.consumer.rebalance;

import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/17
 */
@Data
public class RebalanceResult {

    private String cid;

    private String topic;

    private String group;

    private int queueId;

    private Integer version;

}
