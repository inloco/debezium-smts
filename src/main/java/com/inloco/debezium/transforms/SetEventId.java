package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import java.util.UUID;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class SetEventId implements Transformation {
  private static final String PURPOSE = "Access values to insert new event id field";
  protected static final String EVENT_FIELD_CONFIG = "field";
  protected static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              EVENT_FIELD_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH,
              "The name of the new field for the event id.");

  private String eventIdField;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    if (record == null || record.valueSchema() == null) return record;
    final Struct recordValue = requireStruct(record.value(), PURPOSE);
    if (recordValue.schema().field(eventIdField) != null) return record;
    final Schema updatedRecordSchema = updateSchema(recordValue);
    final Struct updatedRecordValue = addEventId(recordValue, updatedRecordSchema);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedRecordSchema,
        updatedRecordValue,
        record.timestamp());
  }

  private Schema updateSchema(Struct recordValue) {
    Schema updatedSchema = schemaUpdateCache.get(recordValue.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(recordValue.schema());
      schemaUpdateCache.put(recordValue.schema(), updatedSchema);
    }
    return updatedSchema;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    builder.field(eventIdField, Schema.OPTIONAL_STRING_SCHEMA);
    return builder.build();
  }

  private Struct addEventId(Struct recordValue, Schema updatedSchema) {
    final Struct updatedRecordValue = new Struct(updatedSchema);
    for (Field field : updatedSchema.fields()) {
      final Object fieldValue =
          field.name().equals(eventIdField)
              ? UUID.randomUUID().toString()
              : recordValue.get(field.name());
      updatedRecordValue.put(field.name(), fieldValue);
    }
    return updatedRecordValue;
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    eventIdField = config.getString(EVENT_FIELD_CONFIG);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }
}
