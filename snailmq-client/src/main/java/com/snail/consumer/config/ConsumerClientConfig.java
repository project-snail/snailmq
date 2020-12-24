package com.snail.consumer.config;

import com.snail.consumer.ack.AckModeEnums;
import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/24
 */
@Data
public class ConsumerClientConfig {

    private Integer ackTimeSeconds = 10 * 60;

    private Integer ackCount = 1;

    private AckModeEnums ackMode = AckModeEnums.RECORD;

}
