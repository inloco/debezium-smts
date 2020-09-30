package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
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

public class SetBeforeAndAfterName implements Transformation {
  private static final String PURPOSE = "Access values to modify namespace";
  private static final String BEFORE_FIELD_NAME = "before";
  private static final String AFTER_FIELD_NAME = "after";

  protected static final String NEW_NAME_CONFIG = "name";
  protected static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              NEW_NAME_CONFIG,
              ConfigDef.Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH,
              "The new name for the internal records in the before and after schemas.");

  private String newName;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    final Struct recordValue = requireStruct(record.value(), PURPOSE);
    Struct beforeValue = recordValue.getStruct(BEFORE_FIELD_NAME);
    Struct afterValue = recordValue.getStruct(AFTER_FIELD_NAME);
    if (beforeValue == null && afterValue == null) return record;

    Schema updatedBeforeSchema = updateSchema(beforeValue);
    Struct updatedBeforeValue = copyValues(beforeValue, updatedBeforeSchema);

    Schema updatedAfterSchema = updateSchema(afterValue);
    Struct updatedAfterValue = copyValues(afterValue, updatedAfterSchema);
    Schema updatedRecordSchema =
        replaceBeforeAndAfterSchemata(
            record.valueSchema(), updatedBeforeSchema, updatedAfterSchema);
    Struct updatedRecordValue =
        replaceBeforeAndAfterValues(
            record, updatedRecordSchema, updatedBeforeValue, updatedAfterValue);
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
    if (recordValue == null) return null;
    Schema updatedSchema = schemaUpdateCache.get(recordValue.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(recordValue.schema());
      schemaUpdateCache.put(recordValue.schema(), updatedSchema);
    }
    return updatedSchema;
  }

  private Schema makeUpdatedSchema(Schema schema) {
    final SchemaBuilder builder = copyBasicsSchemaWithoutName(schema, SchemaBuilder.struct());
    builder.name(newName);
    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    return builder.build();
  }

  private Struct copyValues(Struct recordValue, Schema updatedSchema) {
    if (recordValue == null) return null;
    final Struct updatedRecordValue = new Struct(updatedSchema);
    for (Field field : updatedRecordValue.schema().fields()) {
      updatedRecordValue.put(field.name(), recordValue.get(field));
    }
    return updatedRecordValue;
  }

  private Schema replaceBeforeAndAfterSchemata(
      Schema schema, Schema beforeSchemaReplacement, Schema afterSchemaReplacement) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      if (!field.name().equals(AFTER_FIELD_NAME) && !field.name().equals(BEFORE_FIELD_NAME)) {
        builder.field(field.name(), field.schema());
      }
    }
    if (beforeSchemaReplacement != null) {
      builder.field(BEFORE_FIELD_NAME, beforeSchemaReplacement);
    }
    if (afterSchemaReplacement != null) {
      builder.field(AFTER_FIELD_NAME, afterSchemaReplacement);
    }
    return builder.build();
  }

  private Struct replaceBeforeAndAfterValues(
      ConnectRecord record,
      Schema updatedSchema,
      Struct beforeValueReplacement,
      Struct afterValueReplacement) {
    final Struct updatedRecordValue = new Struct(updatedSchema);

    for (Field field : record.valueSchema().fields()) {
      if (!field.name().equals(AFTER_FIELD_NAME) && !field.name().equals(BEFORE_FIELD_NAME)) {
        updatedRecordValue.put(field.name(), ((Struct) record.value()).get(field));
      }
    }
    if (beforeValueReplacement != null) {
      updatedRecordValue.put(BEFORE_FIELD_NAME, beforeValueReplacement);
    }
    if (afterValueReplacement != null) {
      updatedRecordValue.put(AFTER_FIELD_NAME, afterValueReplacement);
    }
    return updatedRecordValue;
  }

  private SchemaBuilder copyBasicsSchemaWithoutName(Schema source, SchemaBuilder builder) {
    builder.version(source.version());
    builder.doc(source.doc());

    final Map<String, String> params = source.parameters();
    if (params != null) {
      builder.parameters(params);
    }
    return builder;
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
    newName = config.getString(NEW_NAME_CONFIG);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }
}
