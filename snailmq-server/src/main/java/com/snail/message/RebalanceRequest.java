package com.snail.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.rebalance
 * @Description:
 * @date: 2020/12/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class RebalanceRequest {

    private String topic;

    private String group;

}
