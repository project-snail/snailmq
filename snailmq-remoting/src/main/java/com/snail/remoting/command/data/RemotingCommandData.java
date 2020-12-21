package com.snail.remoting.command.data;

import com.snail.store.AbstractStoreItem;
import com.snail.store.StringStoreItem;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command
 * @Description:
 * @date: 2020/12/17
 */
public abstract class RemotingCommandData<T extends RemotingCommandData> extends AbstractStoreItem<RemotingCommandData<T>> {

    public static final RemotingCommandData OK = new RemotingCommandData() {
        private final StringStoreItem OK = new StringStoreItem("");
        @Override
        public ByteBuffer serialize() {
            return OK.serialize();
        }
        @Override
        public int getSize() {
            return OK.getSize();
        }
    };

    @Override
    public RemotingCommandData<T> body() {
        return this;
    }


}
