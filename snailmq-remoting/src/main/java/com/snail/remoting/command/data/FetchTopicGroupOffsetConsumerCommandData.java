package com.snail.remoting.command.data;


import com.snail.consumer.TopicGroupConsumerOffset;

import java.util.List;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/17
 */
public class FetchTopicGroupOffsetConsumerCommandData extends JsonCommandData<FetchTopicGroupOffsetConsumerCommandData> {
    public FetchTopicGroupOffsetConsumerCommandData(List<TopicGroupConsumerOffset> topicGroupConsumerOffsetList) {
        super(topicGroupConsumerOffsetList);
    }
    public FetchTopicGroupOffsetConsumerCommandData(byte[] jsonBytes) {
        super(jsonBytes);
    }
}
