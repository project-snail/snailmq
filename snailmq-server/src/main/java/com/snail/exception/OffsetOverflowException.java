package com.snail.exception;


import static com.snail.remoting.command.type.CommandExceptionStateEnums.OFFSET_OVERFLOW;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.exception
 * @Description:
 * @date: 2020/12/21
 */
public class OffsetOverflowException extends SnailBaseException {
    public OffsetOverflowException() {
        super("非法偏移量，该偏移量大于最大偏移量", OFFSET_OVERFLOW);
    }
}
