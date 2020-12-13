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
public interface StoreItem<T> {

    /**
     * 获取存储对象
     */
    T body();

    /**
     * 获取序列化对象
     */
    ByteBuffer serialize();

    /**
     * 获取数据大小
     */
    int getSize();

}
