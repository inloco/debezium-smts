package com.inloco.debezium.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SetBeforeAndAfterNameTest {
  private static final String ROOT_LEVEL_NAME = "com.my.app";
  private static final String INNER_NAME_WITH_NAMESPACE = "db.namespace.table.Value";

  @Test
  void testSetBeforeAndAfterName_withAllConfigs() {
    Schema schema = createValueSchema();
    Struct value = populateInnerFields(schema, Arrays.asList("before", "after"));
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    Struct transformedRecordValue = requireStruct(transformedRecord.value(), "testing");
    assertThat(transformedRecordValue.getStruct("before").schema().name()).isEqualTo(newName);
    assertThat(transformedRecordValue.getStruct("after").schema().name()).isEqualTo(newName);
    assertThat(transformedRecordValue.schema().name()).isEqualTo(ROOT_LEVEL_NAME);
  }

  @Test
  void testSetBeforeAndAfterName_withoutBeforeEvent() {
    Schema schema = createValueSchema();
    Struct value = populateInnerFields(schema, Collections.singletonList("after"));
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    Struct transformedRecordValue = requireStruct(transformedRecord.value(), "testing");
    assertThat(transformedRecordValue.getStruct("after").schema().name()).isEqualTo(newName);
    assertThat(transformedRecord.valueSchema().field("before")).isNotNull();
    assertThat(transformedRecord.valueSchema().field("before").schema().name()).isEqualTo(newName);
    assertThat(transformedRecordValue.schema().name()).isEqualTo(ROOT_LEVEL_NAME);
  }

  @Test
  void testSetBeforeAndAfterName_withoutAfterEvent() {
    Schema schema = createValueSchema();
    Struct value = populateInnerFields(schema, Collections.singletonList("before"));
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    Struct transformedRecordValue = requireStruct(transformedRecord.value(), "testing");
    assertThat(transformedRecordValue.getStruct("before").schema().name()).isEqualTo(newName);
    assertThat(transformedRecord.valueSchema().field("after")).isNotNull();
    assertThat(transformedRecord.valueSchema().field("after").schema().name()).isEqualTo(newName);
    assertThat(transformedRecordValue.schema().name()).isEqualTo(ROOT_LEVEL_NAME);
  }

  @Test
  void testSetBeforeAndAfterName_withDeleteEvent() {
    Schema schema = createValueSchema();
    Struct value = populateInnerFields(schema, Collections.emptyList());
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    Struct transformedRecordValue = requireStruct(transformedRecord.value(), "testing");
    assertThat(transformedRecord.valueSchema().field("before")).isNotNull();
    assertThat(transformedRecord.valueSchema().field("before").schema().name()).isEqualTo(newName);
    assertThat(transformedRecord.valueSchema().field("after")).isNotNull();
    assertThat(transformedRecord.valueSchema().field("after").schema().name()).isEqualTo(newName);
    assertThat(transformedRecordValue.schema().name()).isEqualTo(ROOT_LEVEL_NAME);
  }

  @Test
  void testSetBeforeAndAfterName_withNullMessage() {
    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(null);
    assertThat(transformedRecord).isNull();
  }

  @Test
  void testSetBeforeAndAfterName_withInvalidSchema_shouldReturnSameRecord() {
    Schema schema = createNamedSchemaWithAfterOnlySchema();
    Struct value = populateInnerFields(schema, Collections.emptyList());
    SinkRecord record = new SinkRecord("", 0, null, null, schema, value, 0);

    String newName = "com.my.app.internal.Value";
    Map<String, Object> configurations = new HashMap<>();
    configurations.put(SetBeforeAndAfterName.NEW_NAME_CONFIG, newName);
    SetBeforeAndAfterName transform = new SetBeforeAndAfterName();
    transform.configure(configurations);

    ConnectRecord transformedRecord = transform.apply(record);
    assertThat(transformedRecord).isEqualTo(record);

    schema = createNamedSchemaWithAfterOnlySchema();
    value = populateInnerFields(schema, Collections.emptyList());
    record = new SinkRecord("", 0, null, null, schema, value, 0);

    transformedRecord = transform.apply(record);
    assertThat(transformedRecord).isEqualTo(record);
  }

  private Schema createValueSchema() {
    return SchemaBuilder.struct()
        .name(ROOT_LEVEL_NAME)
        .field("after", createInnerSchema())
        .field("before", createInnerSchema())
        .build();
  }

  private Schema createNamedSchemaWithBeforeOnlySchema() {
    return SchemaBuilder.struct()
        .name(ROOT_LEVEL_NAME)
        .field("before", createInnerSchema())
        .build();
  }

  private Schema createNamedSchemaWithAfterOnlySchema() {
    return SchemaBuilder.struct().name(ROOT_LEVEL_NAME).field("after", createInnerSchema()).build();
  }

  private Schema createInnerSchema() {
    return SchemaBuilder.struct()
        .optional()
        .name(INNER_NAME_WITH_NAMESPACE)
        .field("id", Schema.OPTIONAL_STRING_SCHEMA)
        .field("placeholderA", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .field("placeholderB", Schema.OPTIONAL_BOOLEAN_SCHEMA)
        .build();
  }

  private static Struct populateInnerFields(Schema schema, List<String> fields) {
    Struct outerStruct = new Struct(schema);
    for (String field : fields) {
      outerStruct.put(field, populateInnerField(schema, field));
    }
    return outerStruct;
  }

  private static Struct populateInnerField(Schema schema, String field) {
    Struct struct = new Struct(schema.field(field).schema());
    struct.put("placeholderA", true);
    struct.put("placeholderB", false);
    return struct;
  }
}
