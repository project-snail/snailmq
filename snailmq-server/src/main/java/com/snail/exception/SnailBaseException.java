package com.snail.exception;

import com.snail.remoting.command.type.CommandExceptionStateEnums;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.exception
 * @Description:
 * @date: 2020/12/21
 */
public class SnailBaseException extends RuntimeException {

    private CommandExceptionStateEnums exceptionState = CommandExceptionStateEnums.ERROR;

    public SnailBaseException(String message) {
        super(message);
    }

    public SnailBaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SnailBaseException(String message, CommandExceptionStateEnums exceptionState) {
        super(message);
        this.exceptionState = exceptionState;
    }

    public SnailBaseException(String message, Throwable cause, CommandExceptionStateEnums exceptionState) {
        super(message, cause);
        this.exceptionState = exceptionState;
    }

    public CommandExceptionStateEnums getExceptionState() {
        return exceptionState;
    }
}
