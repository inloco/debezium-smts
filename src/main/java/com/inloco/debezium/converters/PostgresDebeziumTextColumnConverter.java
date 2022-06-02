package com.inloco.debezium.converters;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.postgresql.jdbc.PgArray;

@Slf4j
public class PostgresDebeziumTextColumnConverter
    implements CustomConverter<SchemaBuilder, RelationalColumn> {
  private static final String TEXT_COLUMN_TYPE_NAME = "_text";
  private static final String COLUMN_NAME_PROPERTY = "column.list";
  private static final String COLUMN_NAME_PROPERTY_SEPARATOR = ",";

  private SchemaBuilder fieldSchema;
  private Set<String> columnList;

  @Override
  public void configure(Properties props) {
    fieldSchema = SchemaBuilder.array(SchemaBuilder.array(Schema.STRING_SCHEMA));
    columnList = parseColumnList(props.getProperty(COLUMN_NAME_PROPERTY));
  }

  @Override
  public void converterFor(
      RelationalColumn column, ConverterRegistration<SchemaBuilder> registration) {
    if (columnList.contains(column.name())) {
      if (column.typeName().equals(TEXT_COLUMN_TYPE_NAME)) {
        registration.register(fieldSchema, columnValue -> mapToList((PgArray) columnValue));
      } else {
        log.warn(
            "Skipping mapping of column \""
                + column.name()
                + "\" of type \""
                + column.typeName()
                + "\", which does not match expected type \""
                + TEXT_COLUMN_TYPE_NAME
                + "\".");
      }
    }
  }

  private Set<String> parseColumnList(String columnListProp) {
    if (columnListProp == null) {
      return Collections.emptySet();
    }
    return new HashSet<>(Arrays.asList(columnListProp.split(COLUMN_NAME_PROPERTY_SEPARATOR)));
  }

  private List<List<String>> mapToList(PgArray columnValue) {
    try {
      Object columnValueObj = columnValue.getArray();
      if (columnValueObj instanceof String[]) {
        List<String> mappedArray = map1DArray((String[]) columnValueObj);
        return mappedArray.isEmpty() ? List.of() : List.of(mappedArray);
      } else if (columnValueObj instanceof String[][]) {
        return map2DArray((String[][]) columnValueObj);
      }
      log.error("Unknown type for the column value object");
      return null;
    } catch (Exception e) {
      log.error(e.toString());
      return null;
    }
  }

  private List<List<String>> map2DArray(String[][] array) {
    return Arrays.stream(array).map(this::map1DArray).collect(Collectors.toList());
  }

  private List<String> map1DArray(String[] array) {
    return Arrays.asList(array);
  }
}
