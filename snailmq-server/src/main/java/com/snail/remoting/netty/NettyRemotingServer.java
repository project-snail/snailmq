package com.snail.remoting.netty;

import cn.hutool.core.thread.NamedThreadFactory;
import com.snail.config.RemotingServerConfig;
import com.snail.remoting.command.coder.NettyDecoder;
import com.snail.remoting.command.coder.RemotingCommandNettyEncoder;
import com.snail.remoting.command.coder.SyncRemotingCommandNettyEncoder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.netty
 * @Description:
 * @date: 2020/12/17
 */
@Service
public class NettyRemotingServer implements InitializingBean {

    @Autowired
    private RemotingServerConfig remotingServerConfig;

    private ServerBootstrap serverBootstrap;

    private EventLoopGroup eventLoopGroupSelector;

    private EventLoopGroup eventLoopGroupBoss;

    private DefaultEventExecutorGroup defaultEventExecutorGroup;

    @Autowired
    private RemotingCommandInboundHandler remotingCommandInboundHandler;

    @Autowired
    private SyncRemotingCommandInboundHandler syncRemotingCommandInboundHandler;

    @Override
    public void afterPropertiesSet() throws Exception {
        init();
    }

    private void init() {
        this.serverBootstrap = new ServerBootstrap();
        initEventGroup();
        start();
    }

    private void initEventGroup() {
        if (useEpoll()) {
            this.eventLoopGroupSelector = new EpollEventLoopGroup(
                remotingServerConfig.getSelectorThreadSize(),
                new NamedThreadFactory("NettyRemotingServer#eventLoopGroupSelector", false)
            );
            this.eventLoopGroupBoss = new EpollEventLoopGroup(
                1,
                new NamedThreadFactory("NettyRemotingServer#eventLoopGroupBoss", false)
            );
        } else {
            this.eventLoopGroupSelector = new NioEventLoopGroup(
                remotingServerConfig.getSelectorThreadSize(),
                new NamedThreadFactory("NettyRemotingServer#eventLoopGroupSelector", false)
            );
            this.eventLoopGroupBoss = new NioEventLoopGroup(
                1,
                new NamedThreadFactory("NettyRemotingServer#eventLoopGroupBoss", false)
            );
        }
        this.defaultEventExecutorGroup = new DefaultEventExecutorGroup(
            this.remotingServerConfig.getWorkThreadSize(),
            new NamedThreadFactory("NettyRemotingServer#defaultEventExecutorGroup", false)
        );
    }

    private void start() {
        ServerBootstrap serverBootstrap = this.serverBootstrap.group(this.eventLoopGroupBoss, this.eventLoopGroupBoss)
            .channel(useEpoll() ? EpollServerSocketChannel.class : NioServerSocketChannel.class)
            .option(ChannelOption.SO_BACKLOG, 1024)
            .option(ChannelOption.SO_REUSEADDR, true)
            .childOption(ChannelOption.SO_KEEPALIVE, false)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .localAddress(this.remotingServerConfig.getListenPort())
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(
                                defaultEventExecutorGroup,
                                new IdleStateHandler(
                                    0,
                                    0,
                                    remotingServerConfig.getServerChannelMaxIdleTimeSeconds()
                                ),
                                new NettyDecoder(remotingServerConfig.getFrameMaxLength()),
                                new RemotingCommandNettyEncoder(),
                                new SyncRemotingCommandNettyEncoder(),
                                remotingCommandInboundHandler,
                                syncRemotingCommandInboundHandler
                            );
                    }
                }
            );
        ChannelFuture sync = null;
        try {
            sync = serverBootstrap.bind().sync();
        } catch (InterruptedException ignored) {
        }
    }

    public void shutdown() {
        this.defaultEventExecutorGroup.shutdownGracefully();
        this.eventLoopGroupBoss.shutdownGracefully();
        this.eventLoopGroupSelector.shutdownGracefully();
    }

    private boolean useEpoll() {
        return Boolean.TRUE.equals(remotingServerConfig.getUseEpoll())
            && Epoll.isAvailable();
    }

}
