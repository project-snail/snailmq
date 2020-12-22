package com.snail.remoting.command.exception;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.exception
 * @Description:
 * @date: 2020/12/22
 */
public class SyncRemotingCommandTimeOutException extends RuntimeException {
    public SyncRemotingCommandTimeOutException() {
        super("同步命令超时");
    }
}
