package com.snail.remoting.command.data;

import com.snail.request.GetMessageRequest;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/17
 */
public class PullMessageCommandData extends JsonCommandData<PullMessageCommandData> {
    public PullMessageCommandData(GetMessageRequest getMessageRequest) {
        super(getMessageRequest);
    }
}