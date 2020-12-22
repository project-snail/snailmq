package com.snail.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.request
 * @Description:
 * @date: 2020/12/22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class PullNextMessageOffsetRequest {

    private int syncCode;

    private String topic;

    private String group;

    private int queueId;

    //当前最后一条消息的偏移量
    private long offset;

    //重平衡的版本号
    private int version;

}
