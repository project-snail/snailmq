package com.snail.remoting.command.type;

import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.data.RemotingCommandData;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.type
 * @Description:
 * @date: 2020/12/21
 */
public enum CommandExceptionStateEnums {
    OK,
    ERROR,
    OFFSET_OVERFLOW,
    QUEUE_NO_PERMISSIONS,
    MESSAGE_GONE,
    MESSAGE_MISSING
    ;

    public static RemotingCommand buildExRemotingCommand(CommandExceptionStateEnums exceptionState) {
        RemotingCommand remotingCommand = new RemotingCommand(CommandTypeEnums.ERROR, RemotingCommandData.OK);
        remotingCommand.setExceptionState(exceptionState);
        return remotingCommand;
    }

}
