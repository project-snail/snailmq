package com.snail.request;

import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.request
 * @Description:
 * @date: 2020/12/21
 */
@Data
public class UpdateOffsetRequest {

    private String topic;

    private String group;

    private int queueId;

    private long offset;

}
