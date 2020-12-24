package com.snail;

import com.fasterxml.jackson.core.type.TypeReference;
import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.ack.AckModeEnums;
import com.snail.consumer.config.ConsumerClientConfig;
import com.snail.consumer.impl.ConsumerClientServiceImpl;
import com.snail.consumer.listener.PullMessageListener;
import com.snail.message.Message;
import com.snail.message.MessageRecord;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.remoting.command.data.PushMessageCommandData;
import com.snail.remoting.command.data.RebalanceResponseCommandData;
import com.snail.remoting.command.data.RegisterCidCommandData;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.remoting.config.RemotingClientConfig;
import com.snail.remoting.netty.NettyRemotingClient;
import com.snail.request.FetchRebalanceRequest;
import com.snail.result.RebalanceResult;
import io.netty.channel.Channel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail
 * @Description:
 * @date: 2020/12/17
 */
@RunWith(JUnit4.class)
public class TestApplication {
    @Test
    public void testClient() throws InterruptedException {

        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(new RemotingClientConfig());

        Channel channel = nettyRemotingClient.getChannelFuture().sync().channel();

        Message message = new Message();
        message.setTopic("topic");
        message.setKey("key");
        message.setFlag(0);
        message.setBody(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)));

        PushMessageCommandData pushMessageCommandData = new PushMessageCommandData(message);

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.PUSH_MESSAGE,
            pushMessageCommandData
        );

        channel.writeAndFlush(remotingCommand).sync();

    }

    @Test
    public void testSyncCommand() throws InterruptedException {


        Message message = new Message();
        message.setTopic("topic");
        message.setKey("key");
        message.setFlag(0);
        message.setBody(ByteBuffer.wrap("123".getBytes(StandardCharsets.UTF_8)));

        PushMessageCommandData pushMessageCommandData = new PushMessageCommandData(message);

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.PUSH_MESSAGE,
            pushMessageCommandData
        );

        SyncRemotingCommand syncRemotingCommand = new SyncRemotingCommand(remotingCommand);

        ByteBuffer serialize = syncRemotingCommand.serialize();

        SyncRemotingCommand deserialize = SyncRemotingCommand.deserialize(serialize);

        System.out.println(deserialize);

        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(new RemotingClientConfig());

        Channel channel = nettyRemotingClient.getChannelFuture().sync().channel();

        channel.writeAndFlush(syncRemotingCommand).sync();

    }

    @Test
    public void testRegister() throws InterruptedException {

        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(new RemotingClientConfig());

        Channel channel = nettyRemotingClient.getChannelFuture().sync().channel();

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.REGISTER_CID,
            new RegisterCidCommandData("123")
        );

        channel.writeAndFlush(remotingCommand).sync();

        FetchRebalanceRequest fetchRebalanceRequest = new FetchRebalanceRequest();

        fetchRebalanceRequest.setTopic("testTopic");
        fetchRebalanceRequest.setGroup("testGroup");

        RemotingCommand remotingCommand2 = new RemotingCommand(
            CommandTypeEnums.FETCH_REBALANCE_RESULT,
            new RebalanceResponseCommandData(Collections.singletonList(fetchRebalanceRequest))
        );

        channel.writeAndFlush(remotingCommand2).sync();

    }

    @Test
    public void testRegisterSync() throws InterruptedException {
        NettyRemotingClient nettyRemotingClient = new NettyRemotingClient(new RemotingClientConfig());

        Channel channel = nettyRemotingClient.getChannelFuture().sync().channel();

        RemotingCommand remotingCommand = new RemotingCommand(
            CommandTypeEnums.REGISTER_CID,
            new RegisterCidCommandData("123")
        );

        channel.writeAndFlush(remotingCommand).sync();

        FetchRebalanceRequest fetchRebalanceRequest = new FetchRebalanceRequest();

        fetchRebalanceRequest.setTopic("testTopic");
        fetchRebalanceRequest.setGroup("testGroup");

        RemotingCommand remotingCommand2 = new RemotingCommand(
            CommandTypeEnums.FETCH_REBALANCE_RESULT,
            new RebalanceResponseCommandData(Collections.singletonList(fetchRebalanceRequest))
        );

        SyncRemotingCommand syncRemotingCommand = new SyncRemotingCommand(remotingCommand2);

        channel.writeAndFlush(syncRemotingCommand).sync();

        RemotingCommand res = syncRemotingCommand.getRes();

        System.out.println(res);

        List<RebalanceResult> rebalanceResultList = JsonCommandData.deserialize(res.getDataByteBuffer()).getObj(new TypeReference<List<RebalanceResult>>() {
        });

        System.out.println(rebalanceResultList);
    }

    @Test
    public void testSetupClient() {
        RemotingClientConfig remotingClientConfig = new RemotingClientConfig();
        ConsumerClientConfig consumerClientConfig = new ConsumerClientConfig();
        consumerClientConfig.setAckCount(10);
        ConsumerClientServiceImpl service = new ConsumerClientServiceImpl(remotingClientConfig, consumerClientConfig);
        service.addMsgListener(
            "testTopic1", "testConsumer",
            AckModeEnums.COUNT_TIME, messageRecord -> System.out.println(messageRecord.getBody())
        );
        service.startListener();
        new Scanner(System.in).nextInt();
    }

    @Test
    public void testPushMessage() throws InterruptedException {
        RemotingClientConfig remotingClientConfig = new RemotingClientConfig();
        ConsumerClientServiceImpl service = new ConsumerClientServiceImpl(remotingClientConfig);

        for (int i = 1; i < 600; i++) {
            Message message = new Message();
            message.setTopic("testTopic1");
            message.setKey("testConsumer");
            message.setFlag(0);
            String body = i + "条消息";
            message.setBody(ByteBuffer.wrap(body.getBytes(StandardCharsets.UTF_8)));
            service.pushMessage(message, true);
//            service.pushMessage(message, false);
//            Thread.sleep(500);
        }
    }


}
