package com.snail.consumer;

import com.snail.consumer.listener.PullMessageListener;
import com.snail.consumer.listener.PullMessageListenerExecutor;
import com.snail.message.Message;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.SyncRemotingCommand;
import io.netty.channel.ChannelFuture;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer
 * @Description:
 * @date: 2020/12/17
 */
public interface ConsumerClientService {

    void pushMessage(Message message, boolean isSync);

    void addMsgListener(String topic, String group, PullMessageListener listener);

    void addMsgListener(String topic, PullMessageListener listener);

    void startListener();

    boolean registerListenerExecutor(PullMessageListener listener, PullMessageListenerExecutor executor);

    boolean removeListenerExecutor(PullMessageListener listener, PullMessageListenerExecutor executor);

    ChannelFuture sendAsync(RemotingCommand remotingCommand);

    RemotingCommand sendSync(RemotingCommand remotingCommand);

    RemotingCommand sendSync(RemotingCommand remotingCommand, long time, TimeUnit timeUnit);

    RemotingCommand sendSync(SyncRemotingCommand syncRemotingCommand);

    RemotingCommand sendSync(SyncRemotingCommand syncRemotingCommand, long time, TimeUnit timeUnit);

}
