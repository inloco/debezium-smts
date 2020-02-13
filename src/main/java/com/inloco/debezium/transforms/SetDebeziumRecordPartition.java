package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

public class SetDebeziumRecordPartition implements Transformation {
  static final String PARTITIONS_NUMBER_CONFIG = "partitions.number";
  static final String KEY_FIELD_CONFIG = "key.field";

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              PARTITIONS_NUMBER_CONFIG,
              Type.INT,
              ConfigDef.NO_DEFAULT_VALUE,
              Range.between(1, 25),
              ConfigDef.Importance.HIGH,
              "The number of partitions in the output topic.")
          .define(
              KEY_FIELD_CONFIG,
              Type.STRING,
              ConfigDef.NO_DEFAULT_VALUE,
              Importance.HIGH,
              "the key field to use for partitioning");
  private static final String PURPOSE = "Get key value for partitioning";

  private int numPartitions;
  private String keyField;

  @Override
  public ConnectRecord apply(ConnectRecord record) {
    Struct key = requireStruct(record.key(), PURPOSE);
    Serializable keyFieldValue = (Serializable) key.get(keyField);

    byte[] keyBytes = SerializationUtils.serialize(keyFieldValue);
    int recordPartition = Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;

    return record.newRecord(
        record.topic(),
        recordPartition,
        record.keySchema(),
        record.key(),
        record.valueSchema(),
        record.value(),
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
    numPartitions = config.getInt(PARTITIONS_NUMBER_CONFIG);
    keyField = config.getString(KEY_FIELD_CONFIG);
  }
}
