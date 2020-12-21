package com.snail.remoting.command.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command.data
 * @Description:
 * @date: 2020/12/18
 */
public class JsonCommandData<T extends JsonCommandData> extends RemotingCommandData<T> {

    private byte[] jsonBytes;

    public JsonCommandData(Object jsonObj) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.jsonBytes = objectMapper.writeValueAsBytes(jsonObj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("FetchTopicGroupOffsetCommandData 序列化信息异常", e);
        }
    }

    public JsonCommandData(byte[] jsonBytes) {
        this.jsonBytes = jsonBytes;
    }

    @Override
    public ByteBuffer serialize() {
        return serialize(this.jsonBytes);
    }

    @Override
    public int getSize() {
        return 4 + jsonBytes.length;
    }

    public <R> R getObj(Class<R> targetClass) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(
                this.jsonBytes,
                targetClass
            );
        } catch (IOException e) {
            throw new RuntimeException("反序列信息失败", e);
        }
    }

    public <J> J getObj(TypeReference<J> typeReference) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(
                this.jsonBytes,
                typeReference
            );
        } catch (IOException e) {
            throw new RuntimeException("反序列TopicGroupOffset信息失败", e);
        }
    }

    public static <T extends JsonCommandData> JsonCommandData<T> deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        byte[] bodyBytes = new byte[byteBuffer.remaining()];

        byteBuffer.get(bodyBytes);

        return new JsonCommandData<>(bodyBytes);

    }


}
