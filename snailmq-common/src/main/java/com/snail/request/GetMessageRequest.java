package com.snail.request;

import lombok.Data;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/18
 */
@Data
public class GetMessageRequest {

    private String cid;

    private String topic;

    private String group;

    private int queueId;

    private long offset;

//    重平衡的版本号
    private int version;

}
