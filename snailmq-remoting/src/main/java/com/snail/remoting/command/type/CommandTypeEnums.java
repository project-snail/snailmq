package com.snail.remoting.command.type;

import com.snail.remoting.command.data.*;

/**
 * FetchTopicGroupOffset
 *
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.type
 * @Description:
 * @date: 2020/12/17
 */
public enum CommandTypeEnums {

    OK(null),

    ERROR(null),

    //    注册自己 并设置cid
    REGISTER_CID(RegisterCidCommandData.class),

    FETCH_REBALANCE_RESULT(FetchRebalanceResultCommandData.class),

    //    获取组的偏移量
    FETCH_TOPIC_GROUP_OFFSET(FetchTopicGroupOffsetCommandData.class),

    //    拉取消息
    PULL_MESSAGE(PullMessageCommandData.class),

    //    发送消息
    PUSH_MESSAGE(PushMessageCommandData.class),

    //    更新偏移量
    UPDATE_OFFSET(UpdateOffsetCommandData.class);

    private final Class<? extends RemotingCommandData> dataClass;

    CommandTypeEnums(Class<? extends RemotingCommandData> dataClass) {
        this.dataClass = dataClass;
    }


    public Class<? extends RemotingCommandData> getDataClass() {
        return dataClass;
    }
}
