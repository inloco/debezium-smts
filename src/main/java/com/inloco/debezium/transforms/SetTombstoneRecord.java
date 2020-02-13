package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class SetTombstoneRecord implements Transformation {
  static final String FIELD_CONFIG = "value.field";

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              FIELD_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              ConfigDef.Importance.HIGH,
              "The value field will be read and if it's null, a tombstone will be produced.");

  private static final String PURPOSE = "Access field to check if it's null";

  private String field;

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    final Struct value = requireStruct(record.value(), PURPOSE);

    Object nullableField = value.get(field);
    Object newValue = Objects.isNull(nullableField) ? null : record.value();

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        newValue,
        record.timestamp());
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
    field = config.getString(FIELD_CONFIG);
  }
}
