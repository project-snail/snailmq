package com.snail.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/18
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FetchRebalanceRequest {

    private String topic;

    private String group;

}
