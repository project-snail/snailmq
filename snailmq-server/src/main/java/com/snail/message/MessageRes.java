package com.snail.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.message
 * @Description:
 * @date: 2020/12/16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageRes {

//    源消息
    private Message message;

//    下一个消息的偏移量 没有时为-1
    private long nextMsgOffset;
}
