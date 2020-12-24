package com.snail.consumer.listener;

import com.snail.consumer.ack.AckModeEnums;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.listener
 * @Description:
 * @date: 2020/12/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PullMessageListenerContext {

    private String topic;

    private String group;

    private PullMessageListener listener;

    private AckModeEnums ackMode;

}
