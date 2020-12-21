package com.snail.remoting.command;

import com.snail.remoting.command.data.RemotingCommandData;
import com.snail.remoting.command.type.CommandExceptionStateEnums;
import com.snail.remoting.command.type.CommandTypeEnums;
import com.snail.store.AbstractStoreItem;

import java.nio.ByteBuffer;

/**
 * @version V1.0
 * @author: csz
 * @Title
 * @Package: com.snail.remoting.command
 * @Description:
 * @date: 2020/12/17
 */
public class RemotingCommand extends AbstractStoreItem<RemotingCommandData> {

    private final CommandTypeEnums commandType;

    private CommandExceptionStateEnums exceptionState = CommandExceptionStateEnums.OK;

    private RemotingCommandData data;

    private ByteBuffer dataByteBuffer;

    public RemotingCommand(CommandTypeEnums commandType, RemotingCommandData data) {
        this.commandType = commandType;
        this.data = data;
    }

    public RemotingCommand(CommandTypeEnums commandType, ByteBuffer dataByteBuffer) {
        this.commandType = commandType;
        this.dataByteBuffer = dataByteBuffer.slice();
    }

    @Override
    public RemotingCommandData body() {
        return this.data;
    }

    @Override
    public ByteBuffer serialize() {

        ByteBuffer byteBuffer = ByteBuffer.allocate(getSize());

//       commandType(1) + data.len 数据长度
        byteBuffer.putInt(1 + 1 + getDataSize());

        byteBuffer.put((byte) commandType.ordinal());

        byteBuffer.put((byte) exceptionState.ordinal());

        byteBuffer.put(getDataSerialize());

        byteBuffer.flip();

        return byteBuffer.slice();
    }


    /**
     * 总长度(4) 加 类型(1) 加具体数据长度
     */
    @Override
    public int getSize() {
        return 4 + 1 + 1 + getDataSize();
    }

    private int getDataSize() {
        if (data != null) {
            return data.getSize();
        } else {
            return dataByteBuffer.remaining();
        }
    }

    private ByteBuffer getDataSerialize() {
        if (data != null) {
            return data.serialize();
        } else {
            return dataByteBuffer;
        }
    }

    public static RemotingCommand deserialize(ByteBuffer byteBuffer) {

        byteBuffer = getBodyLenByteBuffer(byteBuffer);

        int commandType = byteBuffer.get();

        if (commandType > CommandTypeEnums.values().length) {
            throw new RuntimeException("非法类型");
        }

        CommandTypeEnums commandTypeEnums = CommandTypeEnums.values()[commandType];

        int state = byteBuffer.get();

        if (state > CommandExceptionStateEnums.values().length) {
            throw new RuntimeException("非法类型");
        }

        CommandExceptionStateEnums stateEnums = CommandExceptionStateEnums.values()[state];

        int bodyLen = byteBuffer.getInt();

        ByteBuffer body = ByteBuffer.allocate(4 + bodyLen);

        body.putInt(bodyLen);
        body.put(byteBuffer);

        body.flip();

        RemotingCommand remotingCommand = new RemotingCommand(commandTypeEnums, body);

        remotingCommand.exceptionState = stateEnums;

        return remotingCommand;

    }

    public boolean isOK() {
        return CommandExceptionStateEnums.OK.equals(this.exceptionState);
    }

    public CommandExceptionStateEnums getExceptionState() {
        return exceptionState;
    }

    public void setExceptionState(CommandExceptionStateEnums exceptionState) {
        this.exceptionState = exceptionState;
    }

    public ByteBuffer getDataByteBuffer() {
        return dataByteBuffer.slice();
    }

    public CommandTypeEnums getCommandType() {
        return commandType;
    }
}
