package com.nicoloboschi.javafunctions;

import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcatAvroKeyValue implements Function<GenericObject, String> {

    @Override
    public void initialize(Context context) {
    }

    @Override

    public String process(GenericObject input, Context context) {
        try {
            KeyValue<GenericRecord, GenericRecord> keyValue = (KeyValue<GenericRecord, GenericRecord>) input.getNativeObject();

            final org.apache.avro.generic.GenericRecord keyRecord = (org.apache.avro.generic.GenericRecord) keyValue.getKey().getNativeObject();
            final org.apache.avro.generic.GenericRecord valueRecord = (org.apache.avro.generic.GenericRecord) keyValue.getValue().getNativeObject();
            return Stream.concat(getFieldsValues(keyRecord).stream(), getFieldsValues(valueRecord).stream()).collect(Collectors
                    .joining(","));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private List<String> getFieldsValues(org.apache.avro.generic.GenericRecord record) {
        return record.getSchema().getFields().stream().map(
                f -> record.get(f.name())
        ).map(v -> v == null ? "null" : v.toString()).collect(Collectors.toList());
    }

    @Override
    public void close() {
    }

}
