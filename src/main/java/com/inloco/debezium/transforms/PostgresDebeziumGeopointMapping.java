package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

public class PostgresDebeziumGeopointMapping implements Transformation {
  public static final String LATITUDE_CONFIG = "latitude";
  public static final String LONGITUDE_CONFIG = "longitude";
  public static final String OUTPUT_CONFIG = "output";
  private static final String afterField = "after";

  private static ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              LATITUDE_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.HIGH,
              "the name of the latitude field on the input schema")
          .define(
              LONGITUDE_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.HIGH,
              "the name of the longitude field on the input schema")
          .define(
              OUTPUT_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.HIGH,
              "the name of the output field to add to output schema");

  private static final String PURPOSE = "Get Latitude and Longitude Fields for Parsing";

  private String latitudeField;
  private String longitudeField;
  private String outputField;

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    if (record.value() == null) return record;

    Struct recordValue = requireStruct(record.value(), PURPOSE);
    Struct afterValue = recordValue.getStruct(afterField);

    if (afterValue == null) return record;

    ProcessedAfterField processedAfterField = processAfterField(afterValue);
    Schema updatedAfterSchema = processedAfterField.getSchema();
    Struct updatedAfterValue = processedAfterField.getStruct();

    Schema updatedDebeziumRecordSchema =
        updateDebeziumRecordSchema(record.valueSchema(), updatedAfterSchema);

    final Struct updatedRecordValue = new Struct(updatedDebeziumRecordSchema);

    for (Field field : record.valueSchema().fields()) {
      if (!field.name().equals(afterField))
        updatedRecordValue.put(field.name(), ((Struct) record.value()).get(field));
    }

    updatedRecordValue.put(afterField, updatedAfterValue);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedDebeziumRecordSchema,
        updatedRecordValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
    latitudeField = config.getString(LATITUDE_CONFIG);
    longitudeField = config.getString(LONGITUDE_CONFIG);
    outputField = config.getString(OUTPUT_CONFIG);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  private ProcessedAfterField processAfterField(Struct afterValue) {
    Schema updatedAfterSchema = schemaUpdateCache.get(afterValue.schema());
    if (updatedAfterSchema == null) {
      updatedAfterSchema = updateAfterSchema(afterValue.schema());
      schemaUpdateCache.put(afterValue.schema(), updatedAfterSchema);
    }

    final Struct updatedAfterValue = new Struct(updatedAfterSchema);

    for (Field field : afterValue.schema().fields()) {
      updatedAfterValue.put(field.name(), afterValue.get(field));
    }

    Double lat = afterValue.getFloat64(latitudeField);
    Double lng = afterValue.getFloat64(longitudeField);
    String output = lat.toString() + "," + lng.toString();

    updatedAfterValue.put(outputField, output);

    return new ProcessedAfterField(updatedAfterSchema, updatedAfterValue);
  }

  private Schema updateAfterSchema(Schema schema) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }

    builder.field(outputField, Schema.STRING_SCHEMA);
    return builder.build();
  }

  private Schema updateDebeziumRecordSchema(Schema schema, Schema afterSchemaReplacement) {
    final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      if (!field.name().equals(afterField)) builder.field(field.name(), field.schema());
    }
    builder.field(afterField, afterSchemaReplacement);
    return builder.build();
  }

  private class ProcessedAfterField {
    Schema schema;
    Struct struct;

    public ProcessedAfterField(Schema schema, Struct struct) {
      this.schema = schema;
      this.struct = struct;
    }

    public Schema getSchema() {
      return schema;
    }

    public Struct getStruct() {
      return struct;
    }
  }
}
