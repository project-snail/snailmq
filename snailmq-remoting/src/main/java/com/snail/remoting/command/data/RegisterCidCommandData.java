package com.snail.remoting.command.data;

import com.snail.store.StringStoreItem;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/18
 */
public class RegisterCidCommandData extends RemotingCommandData<FetchTopicGroupOffsetCommandData> {

    private final StringStoreItem cidStoreItem;

    public RegisterCidCommandData(String cid) {
        this.cidStoreItem = new StringStoreItem(cid);
    }

    public RegisterCidCommandData(StringStoreItem cidStoreItem) {
        this.cidStoreItem = cidStoreItem;
    }

    @Override
    public ByteBuffer serialize() {
        return cidStoreItem.serialize();
    }

    public String getCid() {
        return cidStoreItem.body();
    }

    @Override
    public int getSize() {
        return cidStoreItem.getSize();
    }

    public static RegisterCidCommandData deserialize(ByteBuffer byteBuffer) {
        return new RegisterCidCommandData(StringStoreItem.deserialize(byteBuffer));
    }

}
