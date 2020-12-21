package com.snail.store;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @version V1.0
 * @author: csz
 * @Title 携带字节数组的存储对象
 * @Package: com.snail.store
 * @Description:
 * @date: 2020/12/13
 */
public class ByteArrStoreItem extends AbstractStoreItem<byte[]> {

    private byte[] body;

    public ByteArrStoreItem(byte[] body) {
        check(body);
        this.body = body;
    }

    @Override
    public byte[] body() {
        return body;
    }

    @Override
    public ByteBuffer serialize() {
        return serialize(body);
    }

    @Override
    public int getSize() {
        return 4 + body.length;
    }

    public static ByteArrStoreItem deserialize(ByteBuffer byteBuffer) {

        ByteBuffer slice = byteBuffer.slice();

        int bodyLen = slice.getInt();

        byte[] bodyBytes = new byte[bodyLen];

        byteBuffer.get(bodyBytes);

        return new ByteArrStoreItem(bodyBytes);

    }

}
