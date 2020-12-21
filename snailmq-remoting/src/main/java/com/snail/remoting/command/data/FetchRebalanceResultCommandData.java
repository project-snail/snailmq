package com.snail.remoting.command.data;

import com.snail.request.FetchRebalanceRequest;

import java.nio.ByteBuffer;
import java.util.List;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.type
 * @Description:
 * @date: 2020/12/18
 */
public class FetchRebalanceResultCommandData extends JsonCommandData<FetchRebalanceResultCommandData> {

    public FetchRebalanceResultCommandData(List<FetchRebalanceRequest> jsonObj) {
        super(jsonObj);
    }

    public FetchRebalanceResultCommandData(byte[] jsonBytes) {
        super(jsonBytes);
    }

    public static FetchRebalanceResultCommandData deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        byte[] bodyBytes = new byte[byteBuffer.remaining()];

        byteBuffer.get(bodyBytes);

        return new FetchRebalanceResultCommandData(bodyBytes);

    }

}
