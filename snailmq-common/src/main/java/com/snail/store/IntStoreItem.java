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
public class IntStoreItem extends AbstractStoreItem<Integer> {

    private final int body;

    public IntStoreItem(int body) {
        check(body);
        this.body = body;
    }

    @Override
    public Integer body() {
        return body;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putInt(body);
        byteBuffer.flip();
        return byteBuffer;
    }

    @Override
    public int getSize() {
        return 4;
    }

    public static IntStoreItem deserialize(ByteBuffer byteBuffer) {
        return new IntStoreItem(byteBuffer.slice().getInt());
    }

}
