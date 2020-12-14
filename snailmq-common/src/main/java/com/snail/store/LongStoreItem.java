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
public class LongStoreItem extends AbstractStoreItem<Long> {

    private final long body;

    public LongStoreItem(long body) {
        check(body);
        this.body = body;
    }

    @Override
    public Long body() {
        return body;
    }

    @Override
    public ByteBuffer serialize() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putLong(body);
        return byteBuffer;
    }

    @Override
    public int getSize() {
        return 8;
    }

    public static LongStoreItem deserialize(ByteBuffer byteBuffer) {
        return new LongStoreItem(byteBuffer.slice().getLong());
    }

}
