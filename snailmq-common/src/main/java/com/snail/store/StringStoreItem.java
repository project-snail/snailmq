package com.snail.store;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;

/**
 * @version V1.0
 * @author: csz
 * @Title 携带字符串的存储对象
 * @Package: com.snail.store
 * @Description:
 * @date: 2020/12/13
 */
public class StringStoreItem extends AbstractStoreItem<String> {

    private final String body;

    private byte[] bodyBytes;

    private static final StringStoreItem EMPTY = new StringStoreItem("");

    public StringStoreItem(String body) {
        check(body);
        this.body = body;
    }

    @Override
    public String body() {
        return body;
    }

    private byte[] getBodyBytes() {
        if (this.bodyBytes == null) {
            this.bodyBytes = body.getBytes();
        }
        return this.bodyBytes;
    }

    @Override
    public ByteBuffer serialize() {

        byte[] bodyBytes = getBodyBytes();

        return serialize(bodyBytes);

    }

    @Override
    public int getSize() {
        return 4 + getBodyBytes().length;
    }

    public static StringStoreItem deserialize(ByteBuffer byteBuffer) {

        ByteBuffer slice = byteBuffer.slice();

        int bodyLen = slice.getInt();

        if (bodyLen == 0) {
            return EMPTY;
        }

        CharBuffer charBuffer = StandardCharsets.UTF_8.decode((ByteBuffer) slice.slice().limit(bodyLen));

        return new StringStoreItem(charBuffer.toString());

    }

}
