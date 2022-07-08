package com.nicoloboschi.javasink;

import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.TopicMessageIdImpl;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

@Connector(
        name = "nicolo-sink",
        type = IOType.SINK,
        help = "",
        configClass = MySink.Config.class
)
@Slf4j
public class MySink implements Sink<GenericObject> {

    public static class Config {}

    @Override
    public void write(Record<GenericObject> record) throws Exception {
        printRecordInfo(record);

    }

    private void printRecordInfo(Record<GenericObject> sourceRecord) {

        if (sourceRecord.getMessage().isPresent()) {
            MessageId messageId = sourceRecord.getMessage().get().getMessageId();
            MessageIdImpl msgId = (MessageIdImpl) ((messageId instanceof TopicMessageIdImpl)
                    ? ((TopicMessageIdImpl) messageId).getInnerMessageId()
                    : messageId);
            System.out.println("got source of class: " + msgId.getClass().getName());
        } else {
            System.out.println("got source record null!");
        }
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    }

    @Override
    public void close() throws Exception {
    }


}
