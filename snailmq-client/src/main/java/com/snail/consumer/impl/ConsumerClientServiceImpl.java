package com.snail.consumer.impl;

import cn.hutool.core.thread.NamedThreadFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.snail.consumer.ConsumerClientService;
import com.snail.consumer.TopicGroupConsumerOffset;
import com.snail.consumer.TopicGroupOffset;
import com.snail.consumer.listener.PullMessageListener;
import com.snail.consumer.listener.PullMessageListenerExecutor;
import com.snail.message.Message;
import com.snail.remoting.command.RemotingCommand;
import com.snail.remoting.command.RemotingCommandFactory;
import com.snail.remoting.command.SyncRemotingCommand;
import com.snail.remoting.command.data.JsonCommandData;
import com.snail.remoting.config.RemotingClientConfig;
import com.snail.remoting.netty.NettyRemotingClient;
import com.snail.request.FetchRebalanceRequest;
import com.snail.result.RebalanceResult;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.consumer.impl
 * @Description:
 * @date: 2020/12/17
 */
@Slf4j
public class ConsumerClientServiceImpl implements ConsumerClientService {

    private NettyRemotingClient nettyRemotingClient;

    private Map<String/* topic */, Map<String/* group */, PullMessageListener>> pullMessageListenerMap;

    private Map<PullMessageListener, PullMessageListenerExecutor> listenerExecutorMap;

    private Map<String/* topic */, Map<String/* group */, Map<Integer/* queueId */, TopicGroupConsumerOffset/* offset */>>> offsetMap;

    private List<RebalanceResult> rebalanceResultList;

    private ThreadPoolExecutor executor;

    private RemotingClientConfig clientConfig;

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new NamedThreadFactory("ConsumerClientServiceScheduledExecutor", true)
    );

    public ConsumerClientServiceImpl(RemotingClientConfig remotingClientConfig) {
        this.clientConfig = remotingClientConfig;
        this.nettyRemotingClient = new NettyRemotingClient(remotingClientConfig);
        this.pullMessageListenerMap = new ConcurrentHashMap<>();
        this.offsetMap = new ConcurrentHashMap<>();
        this.listenerExecutorMap = new ConcurrentHashMap<>();
        this.executor = new ThreadPoolExecutor(
            0, remotingClientConfig.getMaxListenerSize(),
            60, TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            new NamedThreadFactory("snailMqListener", true),
            new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Override
    public void pushMessage(Message message, boolean isSync) {
        RemotingCommand pushMessageCommand = RemotingCommandFactory.pushMessage(message);
        if (isSync) {
            sendSync(pushMessageCommand);
        } else {
            sendAsync(pushMessageCommand);
        }
    }

    @Override
    public void addMsgListener(String topic, String group, PullMessageListener listener) {
        Map<String, PullMessageListener> groupMap = pullMessageListenerMap.computeIfAbsent(
            topic,
            key -> new ConcurrentHashMap<>()
        );
//        TODO rebalance
        groupMap.put(group, listener);
    }

    @Override
    public void addMsgListener(String topic, PullMessageListener listener) {

    }

    @Override
    public void startListener() {
        init();
    }

    private void init() {
        initRegisterCid();
        initScheduled();
    }

    private void initScheduled() {
        scheduledExecutorService.scheduleWithFixedDelay(
            () -> {
                initRebalance();
                initOffset();
                initListener();
            },
            0, 10, TimeUnit.SECONDS
        );
    }

    private void initListener() {

        for (RebalanceResult rebalanceResult : rebalanceResultList) {

            PullMessageListener pullMessageListener = Optional.ofNullable(
                this.pullMessageListenerMap.get(rebalanceResult.getTopic())
            )
                .map(map -> map.get(rebalanceResult.getGroup()))
                .orElse(null);

            PullMessageListenerExecutor executor = this.listenerExecutorMap.get(pullMessageListener);

            if (executor != null) {
                if (executor.isRunning() && rebalanceResult.getVersion().equals(executor.getVersion())) {
                    continue;
                } else {
                    removeListenerExecutor(pullMessageListener, executor);
                }
            }

            TopicGroupConsumerOffset offset = Optional.ofNullable(this.offsetMap.get(rebalanceResult.getTopic()))
                .map(map -> map.get(rebalanceResult.getGroup()))
                .map(map -> map.get(rebalanceResult.getQueueId()))
                .orElse(null);

            executor = new PullMessageListenerExecutor(
                this, rebalanceResult, pullMessageListener, offset
            );

            this.executor.submit(executor);

            registerListenerExecutor(pullMessageListener, executor);

        }

    }

    private void initRegisterCid() {
        RemotingCommand registerCidCommand = RemotingCommandFactory.registerCid(
            UUID.randomUUID().toString()
        );
        try {
            sendAsync(registerCidCommand).sync();
        } catch (InterruptedException e) {
            log.error("initRegisterCid 被中断", e);
            throw new RuntimeException("initRegisterCid 被中断", e);
        }
    }

    private void initRebalance() {

        List<FetchRebalanceRequest> fetchRebalanceRequests = new LinkedList<>();

        for (Map.Entry<String, Map<String, PullMessageListener>> topicEntry : pullMessageListenerMap.entrySet()) {
            for (Map.Entry<String, PullMessageListener> groupEntry : topicEntry.getValue().entrySet()) {
                FetchRebalanceRequest fetchRebalanceRequest = new FetchRebalanceRequest(
                    topicEntry.getKey(),
                    groupEntry.getKey()
                );
                fetchRebalanceRequests.add(fetchRebalanceRequest);
            }
        }

        RemotingCommand fetchRebalanceCommand = RemotingCommandFactory.fetchRebalanceResult(fetchRebalanceRequests);

        RemotingCommand fetchRebalanceRes = sendSync(fetchRebalanceCommand);

        JsonCommandData<JsonCommandData> deserialize = JsonCommandData.deserialize(fetchRebalanceRes.getDataByteBuffer());

        this.rebalanceResultList = deserialize.getObj(new TypeReference<List<RebalanceResult>>() {
        });

    }

    private void initOffset() {

        if (rebalanceResultList.isEmpty()) {
            return;
        }

        List<TopicGroupOffset> topicGroupOffsetList = rebalanceResultList.stream()
            .map(
                rebalanceResult -> new TopicGroupOffset(
                    rebalanceResult.getTopic(),
                    rebalanceResult.getGroup(),
                    rebalanceResult.getQueueId(),
                    null
                )
            ).collect(Collectors.toList());

        RemotingCommand fetchOffsetCommand = RemotingCommandFactory.fetchTopicGroupOffset(topicGroupOffsetList);

        RemotingCommand fetchOffsetRes = sendSync(fetchOffsetCommand);

        List<TopicGroupConsumerOffset> topicGroupOffsetListList = JsonCommandData.deserialize(fetchOffsetRes.getDataByteBuffer()).getObj(
            new TypeReference<List<TopicGroupConsumerOffset>>() {
            }
        );

        for (TopicGroupConsumerOffset topicGroupConsumerOffset : topicGroupOffsetListList) {
            Map<String, Map<Integer, TopicGroupConsumerOffset>> groupMap = offsetMap.computeIfAbsent(
                topicGroupConsumerOffset.getTopic(),
                key -> new ConcurrentHashMap<>()
            );
            Map<Integer, TopicGroupConsumerOffset> queueMap = groupMap.computeIfAbsent(
                topicGroupConsumerOffset.getGroup(),
                key -> new ConcurrentHashMap<>()
            );
            queueMap.put(topicGroupConsumerOffset.getQueueId(), topicGroupConsumerOffset);
        }

    }

    @Override
    public boolean registerListenerExecutor(PullMessageListener listener, PullMessageListenerExecutor executor) {
        if (listener == null || executor == null) {
            throw new IllegalArgumentException("listener或executor 不能为空");
        }
        return this.listenerExecutorMap.put(listener, executor) == null;
    }

    @Override
    public boolean removeListenerExecutor(PullMessageListener listener, PullMessageListenerExecutor executor) {
        if (listener == null || executor == null) {
            throw new IllegalArgumentException("listener或executor 不能为空");
        }
        synchronized (listener) {
            if (this.listenerExecutorMap.get(listener) == executor) {
                this.listenerExecutorMap.remove(listener);
                return true;
            }
            return false;
        }
    }

    @Override
    public ChannelFuture sendAsync(RemotingCommand remotingCommand) {
        Channel channel = nettyRemotingClient.getChannel();
        return channel.writeAndFlush(remotingCommand);
    }

    @Override
    public RemotingCommand sendSync(SyncRemotingCommand syncRemotingCommand) {
        Channel channel = nettyRemotingClient.getChannel();
        channel.writeAndFlush(syncRemotingCommand);
        try {
            return syncRemotingCommand.getRes(this.clientConfig.getSyncMaxWaitTimeSeconds(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("发送同步请求获取请求被中断", e);
            throw new RuntimeException("发送同步请求获取请求被中断", e);
        }
    }

    @Override
    public RemotingCommand sendSync(RemotingCommand remotingCommand) {
        SyncRemotingCommand syncRemotingCommand = new SyncRemotingCommand(remotingCommand);
        return sendSync(syncRemotingCommand);
    }

    @Override
    public RemotingCommand sendSync(RemotingCommand remotingCommand, long time, TimeUnit timeUnit) {
        SyncRemotingCommand syncRemotingCommand = new SyncRemotingCommand(remotingCommand);
        return sendSync(syncRemotingCommand, time, timeUnit);
    }

    @Override
    public RemotingCommand sendSync(SyncRemotingCommand syncRemotingCommand, long time, TimeUnit timeUnit) {
        Channel channel = nettyRemotingClient.getChannel();
        channel.writeAndFlush(syncRemotingCommand);
        try {
            return syncRemotingCommand.getRes(time, timeUnit);
        } catch (InterruptedException e) {
            log.error("发送同步请求获取请求被中断", e);
            throw new RuntimeException("发送同步请求获取请求被中断", e);
        }
    }
}
