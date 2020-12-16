package com.snail.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TopicGroupOffset {

    private String topic;

    private String group;

    private int queueId;

    private Long offset;

}
