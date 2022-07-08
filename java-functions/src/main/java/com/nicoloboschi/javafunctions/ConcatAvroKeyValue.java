package com.nicoloboschi.javafunctions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcatAvroKeyValue implements Function<GenericObject, ConcatAvroKeyValue.Pojo> {

    @Override
    public void initialize(Context context) {
    }

   /* @Override
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
    }*/


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Pojo {
        private String a;
        private String b;
        private Long mylong;
    }
    @Override
    public Pojo process(GenericObject input, Context context) {
        context.getLogger().info("PROCESSING: " + input);
        try {
            KeyValue<GenericRecord, GenericRecord> keyValue = (KeyValue<GenericRecord, GenericRecord>) input.getNativeObject();

            final org.apache.avro.generic.GenericRecord keyRecord = (org.apache.avro.generic.GenericRecord) keyValue.getKey().getNativeObject();
            final org.apache.avro.generic.GenericRecord valueRecord = (org.apache.avro.generic.GenericRecord) keyValue.getValue().getNativeObject();

            return new Pojo("from-function" + (String) keyValue.getKey().getField("a"), (String) keyValue.getValue().getField("b"),
                    (Long) keyValue.getValue().getField("mylong"));
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
