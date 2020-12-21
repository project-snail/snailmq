package com.snail.store;

import com.snail.commit.CommitLog;
import com.snail.commit.CommitQueue;
import com.snail.config.MessageStoreConfig;
import com.snail.consumer.TopicGroupOffset;
import com.snail.exception.CommitQueueOverflowException;
import com.snail.exception.OffsetOverflowException;
import com.snail.exception.SnailBaseException;
import com.snail.message.Message;
import com.snail.message.MessageExt;
import com.snail.message.MessageRes;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.util.FileUtil;
import com.snail.util.Pair;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.store
 * @Description:
 * @date: 2020/12/14
 */
@Slf4j
@Service
public class CommitStore {

    private TreeMap<Long/* offset */, CommitLog> commitLogMap = null;

    private Map<String/* topic@queueId */, TreeMap<Long/* offset */, CommitQueue>> commitQueueMap = null;

    private ReentrantLock reentrantLock = new ReentrantLock();

    private MessageStoreConfig messageStoreConfig;

    public CommitStore(MessageStoreConfig messageStoreConfig) {
        this.messageStoreConfig = messageStoreConfig;
        init();
    }

    private void init() {
        initCommitLog();
        initQueue();
//        增加jvm结束hook 关闭资源
        Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    }

    /**
     * 初始化log对象
     */
    private void initCommitLog() {

//        commitLog存储位置
        String commitLogDirPath = messageStoreConfig.getBaseDirPath() + messageStoreConfig.getCommitLogDirPrefix();

//        log文件夹下所有文件
        File[] commitLogFiles = FileUtil.getDirFilesWithCreateDir(commitLogDirPath);

        this.commitLogMap = Arrays.stream(commitLogFiles)
            .map(
                file -> {
//                    文件名是commitLog最小的偏移量
                    String commitLogFileName = file.getName();
                    Long commitLogFileMinOffset;
                    try {
                        commitLogFileMinOffset = Long.valueOf(commitLogFileName);
                        if (commitLogFileMinOffset < 0) {
                            return null;
                        }
                    } catch (NumberFormatException e) {
                        log.error("commitLog存放文件夹发现异常文件 ", e);
                        return null;
                    }
                    try {
                        return new Pair<>(
                            commitLogFileMinOffset,
                            new CommitLog(
                                commitLogFileMinOffset,
                                file,
                                this.messageStoreConfig.getCommitLogFileSize(),
                                false
                            )
                        );
                    } catch (IOException e) {
                        log.error("创建commitLog对象错误", e);
                        return null;
                    }
                }
            )
            .filter(Objects::nonNull)
            .collect(
                Collectors.toMap(
                    Pair<Long, CommitLog>::getKey,
                    Pair<Long, CommitLog>::getValue,
                    (k1, k2) -> k2,
                    () -> new TreeMap<>(Comparator.reverseOrder())
                )
            );

    }

    /**
     * 初始化queue
     * <p>
     * queue下是topic文件夹
     * topic下是queueId文件夹 文件夹名称为$(queueId) 默认每个topic含有MessageStoreConfig#queueSize数量的queueId文件夹
     * queueId下有保存messageExt的queue索引文件，每个queue索引含有固定数量的messageExt 长度固定20字节 可随机读写
     * queue索引偏移量从0开始
     * 通过偏移量可以算出在那个queue索引中
     * queue索引名称为0的文件中 包含 0 - maxQueueItemSize 的消息
     */
    private void initQueue() {

//        queue存储位置
        String queueDirPath = messageStoreConfig.getBaseDirPath() + messageStoreConfig.getQueueDirPath();

        File[] topicDirs = FileUtil.getDirFilesWithCreateDir(queueDirPath);

        this.commitQueueMap = Arrays.stream(topicDirs)
            .filter(File::isDirectory)
            .map(
//              topic文件夹对象
                topicDir -> {
//                    queueId文件夹
                    File[] queueFiles = topicDir.listFiles();
                    if (queueFiles == null || queueFiles.length == 0) {
                        return null;
                    }
                    return Arrays.stream(queueFiles)
//                        逐个解析成queue对象                        文件夹名称就是topic名称
                        .map(queueFile -> this.parserCommitQueueMap(topicDir.getName(), queueFile))
                        .filter(Objects::nonNull)
                        .map(queueMap -> new Pair<>(topicDir, queueMap))
                        .collect(Collectors.toList());
                }
            )
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(
                Collectors.toMap(
                    pair -> pair.getKey().getName() + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + pair.getValue().getKey(),
                    pair -> pair.getValue().getValue()
                )
            );

    }

    public synchronized void addMessage(Message message) {

//        获取可写的log文件
        CommitLog writeCommitLog = getWriteCommitLogWithCreate(message.getSize());
//        写入 并获取额外信息
        MessageExt messageExt = writeCommitLog.addMessage(message);
//        获取可写的queue文件
        CommitQueue commitQueue = getWriteCommitQueueWithCreate(message.getTopic(), message.getKey());
//        写入queue索引
        commitQueue.addMessageExt(messageExt);
//        try {
//            reentrantLock.lock();
//        } finally {
//            reentrantLock.unlock();
//        }
    }

    /**
     * 获取可写的log对象
     *
     * @param messageSize
     * @return
     */
    private CommitLog getWriteCommitLogWithCreate(int messageSize) {

        CommitLog commitLog;

//        取出最大偏移量的log对象
        Map.Entry<Long, CommitLog> firstEntry = commitLogMap.firstEntry();
        if (firstEntry == null) {
//            一个都没有则创建一个新的
            return createNextCommitLog(null);
        }

        commitLog = firstEntry.getValue();

        if (!commitLog.isCanWrite(messageSize)) {
//            如果这个文件写满了 创建一个新的
            commitLog = createNextCommitLog(commitLog);
        }

        return commitLog;

    }

    /**
     * 创建新的log对象
     *
     * @param lastCommitLog 当前最后一个log对象，为null时，偏移量初始为0
     * @return 新的log对象
     */
    private CommitLog createNextCommitLog(CommitLog lastCommitLog) {

//        获取下一个文件的初始偏移量 同时作为文件名
        Long newOffset = Optional.ofNullable(lastCommitLog)
            .map(lastLog -> lastLog.getStartOffset() + lastLog.getWritePos().get())
            .orElse(0L);

        try {
//            创建新文件对象
            CommitLog commitLog = new CommitLog(
                newOffset,
                new File(
                    messageStoreConfig.getBaseDirPath()
                        + messageStoreConfig.getCommitLogDirPrefix()
                        + String.format("%020d", newOffset)
                ),
                messageStoreConfig.getCommitLogFileSize(),
                true
            );
            commitLogMap.put(newOffset, commitLog);
            return commitLog;
        } catch (IOException e) {
            log.error("创建新commitLog文件失败", e);
            throw new RuntimeException("创建新commitLog文件失败");
        }
    }

    /**
     * 获取可写的queue对象
     *
     * @param topic 主题
     * @param key   客户端自定义key
     * @return queue可写对象
     */
    private CommitQueue getWriteCommitQueueWithCreate(String topic, String key) {

        int queueId;

//        TODO 换好点的
//        获取key对应的queueId
        if (StringUtils.isBlank(key)) {
            queueId = ((int) Math.floor(Math.random() * 10)) % messageStoreConfig.getQueueSize();
        } else {
            queueId = key.hashCode() % messageStoreConfig.getQueueSize();
        }

        String topicQueueIdKey = topic + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + queueId;

        TreeMap<Long/* queueId */, CommitQueue> queueIndexMap = this.commitQueueMap.get(topicQueueIdKey);

        if (queueIndexMap == null) {
//            没有则新创建
            return createNextCommitQueue(topic, queueId, null);
        }

        CommitQueue commitQueue = queueIndexMap.firstEntry().getValue();

        if (!commitQueue.getIsWritable()) {
//            不可写时新创建
            commitQueue = createNextCommitQueue(topic, queueId, commitQueue.getStartOffset());
        }

        return commitQueue;

    }

    /**
     * 创建新的queue存储对象 用于最后一个存满了 或者 新开始的queue
     *
     * @param topic      主题
     * @param queueId    id
     * @param lastOffset 上一个的偏移量 可为null
     * @return 新的queue对象
     */
    private CommitQueue createNextCommitQueue(String topic, int queueId, Long lastOffset) {
        try {

//            和创建log 大致相同
//            获取新的偏移量
            long newOffset = Optional.ofNullable(lastOffset)
                .map(offset -> offset + this.messageStoreConfig.getMaxQueueItemSize())
                .orElse(0L);

            String newCommitQueueFileName = String.format(
                "%09d",
                newOffset
            );

//            创建新的queueId对象
            CommitQueue commitQueue = new CommitQueue(
                topic,
                queueId,
                newOffset,
                new File(
                    messageStoreConfig.getBaseDirPath()
                        + messageStoreConfig.getQueueDirPath()
                        + File.separator + topic
                        + File.separator + queueId
                        + File.separator + newCommitQueueFileName
                ),
                messageStoreConfig.getMaxQueueItemSize(),
                true
            );

            String topicQueueIdKey = topic + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + queueId;
            TreeMap<Long, CommitQueue> queueIndexMap = this.commitQueueMap.computeIfAbsent(
                topicQueueIdKey,
                k -> new TreeMap<>(Comparator.reverseOrder())
            );

            queueIndexMap.put(newOffset, commitQueue);
            return commitQueue;
        } catch (IOException e) {
            log.error("创建新queueQueue失败", e);
//            TODO 自定义Ex
            throw new RuntimeException("创建新queueQueue失败");
        }

    }

    /**
     * 获取queue对象的时候应该保证还有空间能写入
     */
    @Deprecated
    private void doAddMessageExt(CommitQueue commitQueue, MessageExt messageExt) {

        try {
            commitQueue.addMessageExt(messageExt);
        } catch (CommitQueueOverflowException e) {
            log.info(
                "commitQueue文件写满，切换至下一个文件 queueId: {}, offset: {}",
                commitQueue.getQueueId(),
                commitQueue.getStartOffset()
            );
            CommitQueue nextCommitQueue = createNextCommitQueue(
                commitQueue.getTopic(),
                commitQueue.getQueueId(),
                commitQueue.getStartOffset()
            );
//            TODO 递归
            doAddMessageExt(nextCommitQueue, messageExt);
        }

    }

    /**
     * 获取消息
     *
     * @param topic   主题名
     * @param queueId queueId
     * @param offset  偏移量(相对于queue)
     * @return 消息
     */
    public MessageRes getMessage(String topic, int queueId, Long offset) {

//        找到偏移量对应的queue
        CommitQueue commitQueue = findCommitQueueByOffset(topic, queueId, offset);

//        无此消息队列
        if (commitQueue == null) {
            return new MessageRes(null, -1);
        }

//        找到下一个消息的偏移量
        long nextMessageOffset = findNextMessageOffset(topic, queueId, commitQueue, offset);

//        取出额外信息 包含消息在log中的偏移量
        MessageExt messageExt = commitQueue.getMessageExt(offset);

//        获取偏移量对应的log对象
        CommitLog commitLog = findCommitLogByOffset(messageExt.getCommitLogOffset());

//        从log对象中获取消息
        Message message = commitLog.getMessage(messageExt.getCommitLogOffset());

        return new MessageRes(message, nextMessageOffset);

    }

    public MessageRes getNextMsgOffset(String topic, int queueId, long offset) {

//        找到偏移量对应的queue
        CommitQueue commitQueue = findCommitQueueByOffset(topic, queueId, offset);

//        找到下一个消息的偏移量
        long nextMessageOffset = findNextMessageOffset(topic, queueId, commitQueue, offset);

        return new MessageRes(null, nextMessageOffset);

    }

    private long findNextMessageOffset(String topic, int queueId, CommitQueue commitQueue, Long offset) {

        long maxQueueOffset = commitQueue == null ? -1 : commitQueue.getStartOffset() + commitQueue.getWritePos();

//        如果当前队列还有存有下一个消息
        if (maxQueueOffset > offset + 1) {
            return offset + 1;
        }

        TreeMap<Long/* queueId */, CommitQueue> queueIndexMap = this.commitQueueMap.get(topic + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + queueId);

//        无当前消息队列
        if (queueIndexMap == null) {
            return -1;
        }

//        如果没有了，查找至少有着比offset + 1偏移量还大的队列
        Long find = binarySearchOffset(offset + 1, queueIndexMap.descendingKeySet());

        CommitQueue nextMessageCommitQueue = queueIndexMap.get(find);

//        如果没有 返回-1 代表暂时没有下一个消息了
        return Optional.ofNullable(nextMessageCommitQueue)
            .map(CommitQueue::getStartOffset)
            .map(startOffset -> startOffset < offset + 1 ? -1 : startOffset)
            .orElse(-1L);

    }

    /**
     * 从偏移量中找到对应的log对象
     *
     * @param offset 偏移量
     * @return log对象
     */
    private CommitLog findCommitLogByOffset(Long offset) {

        Map.Entry<Long, CommitLog> firstCommitLogEntry = this.commitLogMap.firstEntry();

//        判断偏移量是否合法
        if (
            firstCommitLogEntry == null ||
                Optional.of(firstCommitLogEntry.getValue())
                    .map(log -> log.getStartOffset() + log.getWritePos().get())
                    .orElse(-1L) <= offset
        ) {
            throw new SnailBaseException(
                String.format("非法偏移量，该偏移量大于最大偏移量 %d", offset),
                CommandExceptionStateEnums.OFFSET_OVERFLOW
            );
        }

        CommitLog commitLog = firstCommitLogEntry.getValue();

//        如果含有最大偏移量的log对象 包含该消息 就不用去之前的log对象中寻找了
        if (commitLog.getStartOffset() < offset) {
            return commitLog;
        }

//        使用二分查找 找出可能在哪个log对象中
        Long find = binarySearchOffset(offset, this.commitLogMap.descendingKeySet());

        commitLog = this.commitLogMap.get(find);

        if (commitLog.getStartOffset() > offset) {
            throw new SnailBaseException("该消息可能已被清除", CommandExceptionStateEnums.MESSAGE_GONE);
        }

        if (commitLog.getStartOffset() + commitLog.getWritePos().get() <= offset) {
            throw new SnailBaseException("消息文件缺失", CommandExceptionStateEnums.MESSAGE_MISSING);
        }

        return commitLog;

    }

    /**
     * 构建offset查找queue
     * 和查找log对象大致相同，只有查找历史消息不一致
     *
     * @param topic   主题
     * @param queueId id
     * @param offset  偏移量
     * @return queue对象
     */
    private CommitQueue findCommitQueueByOffset(String topic, Integer queueId, Long offset) {

        TreeMap<Long/* queueId */, CommitQueue> queueIndexMap = this.commitQueueMap.get(topic + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + queueId);

        if (queueIndexMap == null) {
            return null;
        }

        Map.Entry<Long, CommitQueue> firstEntry = queueIndexMap.firstEntry();

        if (
            firstEntry == null || firstEntry.getValue() == null ||
                Optional.of(firstEntry.getValue())
                    .map(queue -> queue.getStartOffset() + queue.getWritePos())
                    .orElse(-1L) < offset
        ) {
            throw new OffsetOverflowException();
        }

        CommitQueue commitQueue = firstEntry.getValue();

        if (commitQueue.getStartOffset() <= offset) {
            return commitQueue;
        }

//        每个消息固定长度 可以根据offset计算，准确知道消息所在queue对象
        commitQueue = queueIndexMap.get(offset - (offset % this.messageStoreConfig.getMaxQueueItemSize()));

        if (commitQueue == null) {
            throw new SnailBaseException("该消息可能已被清除", CommandExceptionStateEnums.MESSAGE_GONE);
        }

        return commitQueue;

//        Long find = binarySearchOffset(offset, queueIndexMap.descendingKeySet());

//        return queueIndexMap.get(find);

    }

    /**
     * 二分查找实现
     */
    private Long binarySearchOffset(Long offset, Collection<Long> offsetList) {

        ArrayList<Long> findList;

        if (offsetList instanceof ArrayList) {
            findList = (ArrayList<Long>) offsetList;
        } else {
            findList = new ArrayList<>(offsetList);
        }

        int left = 0, right = offsetList.size(), mid = (right + left) >> 1;

        while (left < right) {

            Long midOffset = findList.get(mid);
            if (midOffset > offset) {
                right = mid;
            } else if (mid + 1 >= offsetList.size() || offset < findList.get(mid + 1)) {
                return findList.get(mid);
            } else {
                left = mid;
            }

            mid = (right + left) >> 1;

        }

        return findList.get(mid);

    }

    /**
     * 根据文件和topic解析出queue对象
     *
     * @param topic          主题
     * @param queueIdDirFile queue所在文件夹
     * @return Map<queueId, Map < offset, queue>>
     */
    private Pair<Integer, TreeMap<Long, CommitQueue>> parserCommitQueueMap(String topic, File queueIdDirFile) {

        if (queueIdDirFile == null || !queueIdDirFile.isDirectory()) {
            log.error(
                String.format(
                    "queue存放文件夹发现异常文件 %s",
                    Optional.ofNullable(queueIdDirFile).map(File::getName).orElse("UNKNOWN")
                )
            );
            return null;
        }

        Integer queueId;

        try {
            String FileName = queueIdDirFile.getName();
            queueId = Integer.valueOf(FileName);
        } catch (NumberFormatException e) {
            log.error(String.format("queue存放文件夹发现异常文件 %s ", queueIdDirFile.getName()), e);
            return null;
        }

        File[] queueIndexFiles = FileUtil.getDirFilesWithCreateDir(queueIdDirFile);

        TreeMap<Long, CommitQueue> queueTreeMap = Arrays.stream(queueIndexFiles)
            .map(
                queueIndexFile -> {
                    Long queueIndexOffset;
                    try {
                        queueIndexOffset = Long.valueOf(queueIndexFile.getName());
                    } catch (NumberFormatException e) {
                        log.error(String.format("queue索引存放文件夹发现异常文件 %s ", queueIndexFile.getName()), e);
                        return null;
                    }
                    try {
                        return new Pair<>(
                            queueIndexOffset,
                            new CommitQueue(
                                topic,
                                queueId,
                                queueIndexOffset,
                                queueIndexFile,
                                messageStoreConfig.getMaxQueueItemSize(),
                                false
                            )
                        );
                    } catch (IOException e) {
                        log.error("创建commitQueue对象失败", e);
                        return null;
                    }
                }
            )
            .filter(Objects::nonNull)
            .collect(
                Collectors.toMap(
                    Pair<Long, CommitQueue>::getKey,
                    Pair<Long, CommitQueue>::getValue,
                    (k1, k2) -> k2,
                    () -> new TreeMap<>(Comparator.reverseOrder())
                )
            );

        return new Pair<>(queueId, queueTreeMap);

    }

    public void shutdown() {

        this.commitLogMap.values().forEach(CommitLog::shutdown);
        this.commitQueueMap.values().forEach(
            queueMap -> queueMap.values().forEach(CommitQueue::shutdown)
        );

    }


    public List<TopicGroupOffset> getQueueMinOffset(Collection<TopicGroupOffset> topicGroupOffsetList) {
        return topicGroupOffsetList.stream()
            .map(
                topicGroupOffset -> {

                    TopicGroupOffset groupOffset = new TopicGroupOffset(
                        topicGroupOffset.getTopic(),
                        topicGroupOffset.getGroup(),
                        topicGroupOffset.getQueueId(),
                        -1L
                    );

                    String topicQueueKey = topicGroupOffset.getTopic() + MessageStoreConfig.TOPIC_QUEUE_SEPARATOR + topicGroupOffset.getQueueId();
                    Long minOffset = Optional.ofNullable(this.commitQueueMap.get(topicQueueKey))
                        .map(TreeMap::lastKey)
                        .orElse(-1L);

                    groupOffset.setOffset(minOffset);

                    return groupOffset;

                }
            )
            .collect(Collectors.toList());

    }
}
