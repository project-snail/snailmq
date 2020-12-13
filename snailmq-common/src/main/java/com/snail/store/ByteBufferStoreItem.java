package com.snail.store;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title 携带字节数组的存储对象
 * @Package: com.snail.store
 * @Description:
 * @date: 2020/12/13
 */
public class ByteBufferStoreItem extends AbstractStoreItem<ByteBuffer> {

    private ByteBuffer body;

    public ByteBufferStoreItem(ByteBuffer body) {
        check(body);
        this.body = body;
    }

    @Override
    public ByteBuffer body() {
        return body;
    }

    @Override
    public ByteBuffer serialize() {

        ByteBuffer byteBuffer = this.body instanceof DirectBuffer ?
            ByteBuffer.allocateDirect(
                4 + body.limit()
            )
            :
            ByteBuffer.allocate(
                4 + body.limit()
            );

        byteBuffer.putInt(body.limit());
        byteBuffer.put(body);

        byteBuffer.flip();

        return byteBuffer;

    }

    @Override
    public int getSize() {
        return 4 + body.limit();
    }

    public static ByteBufferStoreItem deserialize(ByteBuffer byteBuffer) {

        ByteBuffer slice = byteBuffer.slice();

        int bodyLen = slice.getInt();

        ByteBuffer body = slice.slice();

        body.limit(bodyLen);

        return new ByteBufferStoreItem(body);

    }

}
