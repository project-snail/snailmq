package com.snail.remoting.command.data;

import com.snail.request.UpdateOffsetRequest;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/21
 */
public class UpdateOffsetCommandData extends JsonCommandData<UpdateOffsetCommandData> {
    public UpdateOffsetCommandData(UpdateOffsetRequest request) {
        super(request);
    }
    public UpdateOffsetCommandData(byte[] jsonBytes) {
        super(jsonBytes);
    }
}
