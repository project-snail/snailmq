package com.snail.store;

import com.snail.commit.CommitLog;
import com.snail.commit.CommitQueue;
import com.snail.config.MessageStoreConfig;
import com.snail.message.Message;
import com.snail.message.MessageExt;
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

    public void addMessage(Message message) {
        try {
            reentrantLock.lock();
            CommitLog writeCommitLog = getWriteCommitLog();
            MessageExt messageExt = writeCommitLog.addMessage(message);
            CommitQueue commitQueue = getCommitLogWithCreate(message.getTopic(), message.getKey());
            commitQueue.addMessageExt(messageExt);
        } finally {
            reentrantLock.unlock();
        }
    }

    public Message getMessage(String topic, String queueId, Long offset) {

        CommitQueue commitQueue = findCommitQueueByOffset(topic, queueId, offset);

        MessageExt messageExt = commitQueue.getMessageExt(Math.toIntExact(offset));

        CommitLog commitLog = findCommitLogByOffset(messageExt.getCommitLogOffset());

        return commitLog.getMessage(messageExt.getCommitLogOffset());

    }

    private CommitLog findCommitLogByOffset(Long offset) {

        Map.Entry<Long, CommitLog> commitLogEntry = this.commitLogMap.firstEntry();
        CommitLog commitLog = commitLogEntry.getValue();

        if (commitLogEntry.getKey() < offset) {
            if (commitLogEntry.getKey() + commitLog.getWritePos().get() <= offset) {
                throw new RuntimeException("非法偏移量，该偏移量大于最大偏移量");
            }
            return commitLog;
        }

        Long find = binarySearchOffset(offset, this.commitLogMap.descendingKeySet());

        return this.commitLogMap.get(find);

    }

    private CommitQueue findCommitQueueByOffset(String topic, String queueId, Long offset) {

        TreeMap<Long, CommitQueue> queueIndexMap = this.commitQueueMap.get(topic + "@" + queueId);

        Long find = binarySearchOffset(offset, queueIndexMap.descendingKeySet());

        return queueIndexMap.get(find);

    }

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
            } else if (mid + 1 >= offsetList.size() || findList.get(mid + 1) > offset) {
                return midOffset;
            } else {
                left = mid;
            }

            mid = (right + left) >> 1;

        }

        return findList.get(mid);

    }


    private CommitQueue getCommitLogWithCreate(String topic, String key) {

        int queueId;

        if (StringUtils.isBlank(key)) {
            queueId = ((int) Math.floor(Math.random() * 10)) % messageStoreConfig.getQueueSize();
        } else {
            queueId = key.hashCode() % messageStoreConfig.getQueueSize();
        }

        String topicQueueIdKey = topic + "@" + queueId;

        TreeMap<Long, CommitQueue> queueIndexMap = this.commitQueueMap.get(topicQueueIdKey);

        if (queueIndexMap != null) {
            return queueIndexMap.firstEntry().getValue();
        }

        try {
            CommitQueue commitQueue = new CommitQueue(
                queueId,
                new File(
                    messageStoreConfig.getBaseDirPath()
                        + messageStoreConfig.getQueueDirPath()
                        + File.separator + topic
                        + File.separator + queueId
                        + File.separator + "000000000"
                ),
                messageStoreConfig.getMaxQueueItemSize(),
                true
            );
            queueIndexMap = new TreeMap<>(Comparator.reverseOrder());
            queueIndexMap.put(0L, commitQueue);
            this.commitQueueMap.put(topicQueueIdKey, queueIndexMap);
            return commitQueue;
        } catch (IOException e) {
            log.error("创建新queueQueue失败", e);
//            TODO 自定义Ex
            throw new RuntimeException("创建新queueQueue失败");
        }

    }

    private CommitLog getWriteCommitLog() {
        Map.Entry<Long, CommitLog> firstEntry = commitLogMap.firstEntry();
        if (firstEntry != null) {
            return firstEntry.getValue();
        }
        try {
            CommitLog commitLog = new CommitLog(
                new File(
                    messageStoreConfig.getBaseDirPath() + messageStoreConfig.getCommitLogDirPrefix() + "00000000000000000000"
                ),
                messageStoreConfig.getCommitLogFileSize(),
                true
            );
            commitLogMap.put(0L, commitLog);
            return commitLog;
        } catch (IOException e) {
            log.error("创建新commitLog文件失败", e);
            throw new RuntimeException("创建新commitLog文件失败");
        }
    }

    private void init() {
        initCommitLog();
        initQueue();
    }

    private void initCommitLog() {

//        commitLog存储位置
        String commitLogDirPath = messageStoreConfig.getBaseDirPath() + messageStoreConfig.getCommitLogDirPrefix();

        File[] commitLogFiles = getDirFiles(commitLogDirPath);

        if (commitLogFiles == null || commitLogFiles.length == 0) {
            return;
        }

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
                            new CommitLog(file, this.messageStoreConfig.getCommitLogFileSize(), false)
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

        File[] topicDirs = getDirFiles(queueDirPath);

        if (topicDirs == null || topicDirs.length == 0) {
            return;
        }

        this.commitQueueMap = Arrays.stream(topicDirs)
            .filter(File::isDirectory)
            .map(
                dir -> {
//                    queueId文件夹
                    File[] queueFiles = dir.listFiles();
                    if (queueFiles == null || queueFiles.length == 0) {
                        return null;
                    }
                    return Arrays.stream(queueFiles)
                        .map(this::parserCommitQueueMap)
                        .filter(Objects::nonNull)
                        .map(queueMap -> new Pair<>(dir, queueMap))
                        .collect(Collectors.toList());
                }
            )
            .filter(Objects::nonNull)
            .flatMap(List::stream)
            .collect(
                Collectors.toMap(
                    pair -> pair.getKey().getName() + "@" + pair.getValue().getKey(),
                    pair -> pair.getValue().getValue()
                )
            );

    }

    private Pair<Integer, TreeMap<Long, CommitQueue>> parserCommitQueueMap(File queueIdDirFile) {

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

        File[] queueIndexFiles = getDirFiles(queueIdDirFile);

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
                                queueId,
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

    private File[] getDirFiles(File dirPathFile) {
        if (dirPathFile.exists() && !dirPathFile.isDirectory()) {
//            TODO 自定义Ex
            throw new RuntimeException("存储文件位置错误");
        }
        if (!dirPathFile.exists()) {
            boolean mkdirsResult = dirPathFile.mkdirs();
            log.info(
                "存储位置不存在，自动创建目录 %s %s", dirPathFile, (mkdirsResult ? "OK" : "FAILED")
            );
            return new File[0];
        }
        return dirPathFile.listFiles();
    }

    private File[] getDirFiles(String dirPath) {
        return getDirFiles(new File(dirPath));
    }

}
