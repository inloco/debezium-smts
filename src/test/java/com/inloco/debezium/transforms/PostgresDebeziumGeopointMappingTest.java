package com.inloco.debezium.transforms;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class PostgresDebeziumGeopointMappingTest {
  @Test
  public void testGeopointMapping_usingStandardProcedure() {
    Schema schema = createValueSchema();
    ConnectRecord originalRecord =
        new SinkRecord(
            "",
            1,
            null,
            null,
            schema,
            populateInnerFields(schema, "id", -30.23, 120.32, "after"),
            0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(PostgresDebeziumGeopointMapping.LATITUDE_CONFIG, "latitude");
    configurations.put(PostgresDebeziumGeopointMapping.LONGITUDE_CONFIG, "longitude");
    configurations.put(PostgresDebeziumGeopointMapping.OUTPUT_CONFIG, "location");
    PostgresDebeziumGeopointMapping transform = new PostgresDebeziumGeopointMapping();
    transform.configure(configurations);

    ConnectRecord outputtedRecord = transform.apply(originalRecord);
    assertThat(
            outputtedRecord
                .valueSchema()
                .field("after")
                .schema()
                .fields()
                .stream()
                .map(field -> field.name())
                .collect(Collectors.toList()))
        .contains("location");
  }
  /*
      A delete message on debezium consists of the operation being set to 'd'
      and the after message being null, in this case we want the transform to ignore the record
      by just returning the record as is.
  */
  @Test
  public void testGeopointMapping_givenDeleteOperation() {
    Schema schema = createValueSchema();
    ConnectRecord originalRecord =
        new SinkRecord(
            "",
            1,
            null,
            null,
            schema,
            populateInnerFields(schema, "id", -30.23, 120.32, "before"),
            0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(PostgresDebeziumGeopointMapping.LATITUDE_CONFIG, "latitude");
    configurations.put(PostgresDebeziumGeopointMapping.LONGITUDE_CONFIG, "longitude");
    configurations.put(PostgresDebeziumGeopointMapping.OUTPUT_CONFIG, "location");
    PostgresDebeziumGeopointMapping transform = new PostgresDebeziumGeopointMapping();
    transform.configure(configurations);

    ConnectRecord outputtedRecord = transform.apply(originalRecord);
    assertThat(outputtedRecord).isEqualTo(originalRecord);
  }

  @Test
  public void testGeopointMapping_givenTombstoneRecord() {
    Schema schema = createValueSchema();
    ConnectRecord originalRecord =
        new SinkRecord("", 1, Schema.STRING_SCHEMA, "testKey", null, null, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(PostgresDebeziumGeopointMapping.LATITUDE_CONFIG, "latitude");
    configurations.put(PostgresDebeziumGeopointMapping.LONGITUDE_CONFIG, "longitude");
    configurations.put(PostgresDebeziumGeopointMapping.OUTPUT_CONFIG, "location");
    PostgresDebeziumGeopointMapping transform = new PostgresDebeziumGeopointMapping();
    transform.configure(configurations);

    ConnectRecord outputtedRecord = transform.apply(originalRecord);
    assertThat(outputtedRecord).isEqualTo(originalRecord);
  }

  private Schema createValueSchema() {
    return SchemaBuilder.struct()
        .name("record")
        .field("after", createGeoSchema("after"))
        .field("before", createGeoSchema("before"))
        .build();
  }

  public static Struct populateInnerFields(
      Schema schema, String id, double lat, double lng, String dataField) {
    Struct struct = new Struct(schema.field(dataField).schema());
    struct.put("id", id);
    struct.put("latitude", lat);
    struct.put("longitude", lng);
    Struct field = new Struct(schema);
    field.put(dataField, struct);
    return field;
  }

  private Schema createGeoSchema(String name) {
    return SchemaBuilder.struct()
        .optional()
        .name(name)
        .field("id", Schema.STRING_SCHEMA)
        .field("latitude", Schema.FLOAT64_SCHEMA)
        .field("longitude", Schema.FLOAT64_SCHEMA)
        .build();
  }
}
