package com.snail.store;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.store
 * @Description:
 * @date: 2020/12/13
 */
public abstract class AbstractStoreItem<T> implements StoreItem<T> {

    protected void check(T body) {
        if (body == null) {
            throw new IllegalArgumentException("存储信息不能为空");
        }
    }

    protected ByteBuffer serialize(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(
            4 + bytes.length
        );

        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);

        byteBuffer.flip();

        return byteBuffer.slice();
    }

    public static ByteBuffer getBodyLenByteBuffer(ByteBuffer byteBuffer) {

        byteBuffer = byteBuffer.slice();

        int bodyLen = byteBuffer.getInt();

        byteBuffer = byteBuffer.slice();

        byteBuffer.limit(bodyLen);

        return byteBuffer;

    }

}
