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
public class TopicGroupConsumerOffset {

    private String topic;

    private String group;

    private int queueId;

//    这个group最后一次消费的消息偏移量
    private Long lastOffset;

//    下一条消息的偏移量
    private Long nextMsgOffset;

}
