package com.inloco.debezium.transforms;

import static com.inloco.debezium.transforms.SetTombstoneRecord.FIELD_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SetTombstoneRecordTest {

  @Test
  void testTombstoneSetForNullField() {
    String fieldName = "aggregate";

    Schema schema = SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build();

    // The field value is not set since it's null.
    Struct value = new Struct(schema);
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(FIELD_CONFIG, fieldName);

    SetTombstoneRecord transform = new SetTombstoneRecord();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    assertThat(transformedRecord.value()).isNull();
  }

  @Test
  void testTombstoneNotSetForNonNullField() {
    String fieldName = "aggregate";
    String fieldValue = "field-value";

    Schema schema = SchemaBuilder.struct().field(fieldName, Schema.STRING_SCHEMA).build();

    Struct value = new Struct(schema);
    value.put(fieldName, fieldValue);
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(FIELD_CONFIG, fieldName);

    SetTombstoneRecord transform = new SetTombstoneRecord();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    assertThat(transformedRecord.value()).isNotNull();
  }
}
