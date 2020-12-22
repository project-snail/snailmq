package com.snail.remoting.command.data;

import com.snail.request.PullNextMessageOffsetRequest;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/22
 */
public class PullNextMessageOffsetCommandData extends JsonCommandData {
    public PullNextMessageOffsetCommandData(PullNextMessageOffsetRequest request) {
        super(request);
    }
    public PullNextMessageOffsetCommandData(byte[] jsonBytes) {
        super(jsonBytes);
    }
}
