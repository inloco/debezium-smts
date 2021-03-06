package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SetEventIdTest {
  @Test
  void testSetEventId_withAllConfigs() {
    Schema schema = createValueSchema();
    Struct value = populateInnerFields(schema, "id");
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String eventIdField = "event_id_test";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetEventId.EVENT_FIELD_CONFIG, eventIdField);
    SetEventId transform = new SetEventId();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    assertThat(requireStruct(transformedRecord.value(), "testing").getString(eventIdField))
        .isNotNull();
  }

  @Test
  void testSetEventId_withAlreadyExistingEventId() {
    String eventIdField = "event_id_test";
    Schema schema = createValueSchemaWithEventId(eventIdField);
    Struct value = populateInnerFieldsWithPreExistingEventId(schema, eventIdField);
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetEventId.EVENT_FIELD_CONFIG, eventIdField);
    SetEventId transform = new SetEventId();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    assertThat(transformedRecord).isEqualTo(record);
  }

  @Test
  void testSetEventId_givenTwoEvents_idsShouldBeUnique() {
    String eventIdField = "event_id_test";
    Schema schema = createValueSchema();
    Struct firstValue = populateInnerFields(schema, "id1");
    SinkRecord firstRecord = new SinkRecord("", 0, null, null, schema, firstValue, 0);

    Struct secondValue = populateInnerFields(schema, "id2");
    SinkRecord secondRecord = new SinkRecord("", 0, null, null, schema, secondValue, 0);

    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetEventId.EVENT_FIELD_CONFIG, eventIdField);
    SetEventId transform = new SetEventId();
    transform.configure(configurations);

    ConnectRecord firstTransformedRecord = transform.apply(firstRecord);
    ConnectRecord secondTransformedRecord = transform.apply(secondRecord);

    assertThat(requireStruct(firstTransformedRecord.value(), "testing").getString(eventIdField))
        .isNotNull()
        .isNotEqualTo(
            requireStruct(secondTransformedRecord.value(), "testing").getString(eventIdField));
  }

  @Test
  void testSetEvenetId_givenNullEvent_shouldReturnNull() {
    String eventIdField = "event_id_test";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetEventId.EVENT_FIELD_CONFIG, eventIdField);
    SetEventId transform = new SetEventId();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(null);

    assertThat(transformedRecord).isNull();
  }

  private Schema createValueSchema() {
    return SchemaBuilder.struct()
        .name("record")
        .field("after", createInnerSchemaWithoutEventId("after"))
        .field("before", createInnerSchemaWithoutEventId("before"))
        .build();
  }

  private Schema createValueSchemaWithEventId(String eventIdField) {
    return SchemaBuilder.struct()
        .name("record")
        .field(eventIdField, Schema.OPTIONAL_STRING_SCHEMA)
        .build();
  }

  private Schema createInnerSchemaWithoutEventId(String name) {
    return SchemaBuilder.struct()
        .optional()
        .name(name)
        .field("id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("placeholderA", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("placeholderB", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
  }

  private static Struct populateInnerFields(Schema schema, String id) {
    Struct struct = new Struct(schema.field("after").schema());
    struct.put("id", id);
    struct.put("placeholderA", true);
    struct.put("placeholderB", false);
    Struct field = new Struct(schema);
    field.put("after", struct);
    return field;
  }

  private static Struct populateInnerFieldsWithPreExistingEventId(
      Schema schema, String eventIdField) {
    Struct struct = new Struct(schema);
    struct.put(eventIdField, "fixed_event_id");
    return struct;
  }
}
