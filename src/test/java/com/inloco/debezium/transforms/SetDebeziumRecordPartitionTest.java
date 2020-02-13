package com.inloco.debezium.transforms;

import static com.inloco.debezium.transforms.SetDebeziumRecordPartition.KEY_FIELD_CONFIG;
import static com.inloco.debezium.transforms.SetDebeziumRecordPartition.PARTITIONS_NUMBER_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SetDebeziumRecordPartitionTest {

  @Test
  void testPartitionCorrectlySet() {
    String fieldName = "aggregate_id";
    String fieldValue = "30";
    int partitionsNumber = 10;

    Schema schema = SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build();

    Struct key = new Struct(schema);
    key.put(fieldName, fieldValue);

    SinkRecord record = new SinkRecord("", 0, schema, key, null, null, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(PARTITIONS_NUMBER_CONFIG, partitionsNumber);
    configurations.put(KEY_FIELD_CONFIG, fieldName);

    SetDebeziumRecordPartition transform = new SetDebeziumRecordPartition();
    transform.configure(configurations);
    ConnectRecord transformedRecord = transform.apply(record);

    byte[] keyBytes = SerializationUtils.serialize(fieldValue);
    int recordPartition = Utils.toPositive(Utils.murmur2(keyBytes)) % partitionsNumber;

    assertThat(transformedRecord.kafkaPartition()).isEqualTo(recordPartition);
  }
}
