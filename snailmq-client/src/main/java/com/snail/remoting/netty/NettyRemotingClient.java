package com.snail.remoting.netty;

import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.remoting.command.coder.NettyDecoder;
import com.snail.remoting.command.coder.RemotingCommandNettyEncoder;
import com.snail.remoting.command.coder.SyncRemotingCommandNettyEncoder;
import com.snail.remoting.config.RemotingClientConfig;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;


/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/17
 */
public class NettyRemotingClient {

    private final Bootstrap bootstrap = new Bootstrap();

    private final EventLoopGroup eventLoopGroupWorker = new NioEventLoopGroup(
        new NamedThreadFactory("NettyRemotingClient", false)
    );

    private ChannelFuture channelFuture;

    private Channel channel;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    private RemotingClientConfig remotingClientConfig;

    private List<Consumer<Channel>> channelAfterInitHook = new LinkedList<>();

    public NettyRemotingClient(RemotingClientConfig remotingClientConfig) {
        this.remotingClientConfig = remotingClientConfig;
        init();
    }

    private void init() {

        defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            4,
            new NamedThreadFactory(
                "ClientEventExecutorGroup",
                false
            )
        );

        this.bootstrap.group(eventLoopGroupWorker)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, false)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, remotingClientConfig.getConnectTimeoutMillis())
            .handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(
                                defaultEventExecutorGroup,
                                new IdleStateHandler(
                                    0,
                                    0,
                                    remotingClientConfig.getServerChannelMaxIdleTimeSeconds()
                                ),
                                new RemotingCommandNettyEncoder(),
                                new SyncRemotingCommandNettyEncoder(),
                                new NettyDecoder(remotingClientConfig.getFrameMaxLength()),
                                new SyncRemotingCommandClientInboundHandler()
                            );
                    }
                }
            );

    }

    private void initChannel() {
        try {
            this.channelFuture = this.bootstrap.connect(
                remotingClientConfig.getServerAddr(),
                remotingClientConfig.getServerPort()
            ).sync();
            this.channel = channelFuture.channel();
            this.channelAfterInitHook.forEach(channelConsumer -> channelConsumer.accept(channel));
        } catch (InterruptedException e) {
            throw new RuntimeException("链接server时被中断", e);
        }
    }

    public void addChannelAfterInitHook(Consumer<Channel> channelConsumer) {
        this.channelAfterInitHook.add(channelConsumer);
    }

    public Channel getChannel() {
        if (channel != null && channel.isWritable()) {
            return channel;
        }
        initChannel();
        return channel;
    }

    public ChannelFuture getChannelFuture() {
        return channelFuture;
    }
}
