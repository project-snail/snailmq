package com.snail.util;

import com.snail.store.StoreItem;

import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * @version V1.0
 * @author: csz
 * @Title 存储单元类
 * @Package: com.snail.util
 * @Description:
 * @date: 2020/12/13
 */
public enum StoreItemUtil {
    ;

    /**
     * 移动buffer指针
     * @param byteBuffer 数据源
     * @param storeItem storeItem
     */
    public static void movePos(ByteBuffer byteBuffer, StoreItem storeItem) {
        byteBuffer.position(
            byteBuffer.position() + storeItem.getSize()
        );
    }

    /**
     * 解析并会自动移动指针 移动的距离为解析出来的StoreItem
     * @param byteBuffer 数据源
     * @param bufferFun 转换方法
     * @param <T> StoreItem
     * @return StoreItem
     */
    public static <T extends StoreItem> T deserializeWithMovePost(ByteBuffer byteBuffer, Function<ByteBuffer, T> bufferFun) {
        T storeItem = bufferFun.apply(byteBuffer);
        movePos(byteBuffer, storeItem);
        return storeItem;
    }

}
