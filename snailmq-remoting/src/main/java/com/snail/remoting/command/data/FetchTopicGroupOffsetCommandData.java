package com.snail.remoting.command.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snail.consumer.TopicGroupOffset;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/17
 */
public class FetchTopicGroupOffsetCommandData extends RemotingCommandData<FetchTopicGroupOffsetCommandData> {

    private byte[] topicGroupOffsetsJsonBytes;

    public FetchTopicGroupOffsetCommandData(List<TopicGroupOffset> topicGroupOffsets) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.topicGroupOffsetsJsonBytes = objectMapper.writeValueAsBytes(topicGroupOffsets);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("FetchTopicGroupOffsetCommandData 序列化信息异常", e);
        }
    }

    public FetchTopicGroupOffsetCommandData(byte[] topicGroupOffsetsJsonBytes) {
        this.topicGroupOffsetsJsonBytes = topicGroupOffsetsJsonBytes;
    }

    @Override
    public ByteBuffer serialize() {
        return super.serialize(this.topicGroupOffsetsJsonBytes);
    }

    /**
     * 总长度 加 data长度
     * @return
     */
    @Override
    public int getSize() {
        return 4 + this.topicGroupOffsetsJsonBytes.length;
    }

    public List<TopicGroupOffset> getTopicGroupOffsets() {

        ObjectMapper objectMapper = new ObjectMapper();

        try {
            return objectMapper.readValue(
                this.topicGroupOffsetsJsonBytes,
                new TypeReference<List<TopicGroupOffset>>() {
                }
            );
        } catch (IOException e) {
            throw new RuntimeException("反序列TopicGroupOffset信息失败", e);
        }

    }

    public static FetchTopicGroupOffsetCommandData deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        byte[] bodyBytes = new byte[byteBuffer.remaining()];

        byteBuffer.get(bodyBytes);

        return new FetchTopicGroupOffsetCommandData(bodyBytes);

    }

}
