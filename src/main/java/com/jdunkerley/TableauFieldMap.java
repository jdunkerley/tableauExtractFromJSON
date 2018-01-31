package com.jdunkerley;

import com.tableausoftware.common.Type;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class TableauFieldMap {
    private final Map<String, Type> fieldMap;

    TableauFieldMap(Stream<Map<String, Object>> stream) {
        this.fieldMap = stream.reduce(new HashMap<>(), TableauFieldMap ::fieldMapAccumulator, TableauFieldMap ::fieldMapCombiner);
    }

    private static Map<String, Type> fieldMapAccumulator(Map<String, Type> current, Map<String, Object> objectMap) {
        Map<String, Type> fieldTypes = new HashMap<>();
        for (String fieldName : objectMap.keySet()) {
            Object fieldValue = objectMap.get(fieldName);

            if (fieldValue == null) {
                fieldTypes.put(fieldName, null);
            } else if (fieldValue instanceof Boolean) {
                fieldTypes.put(fieldName, Type.BOOLEAN);
            } else if (fieldValue instanceof Byte) {
                fieldTypes.put(fieldName, Type.INTEGER);
            } else if (fieldValue instanceof Short) {
                fieldTypes.put(fieldName, Type.INTEGER);
            } else if (fieldValue instanceof Integer) {
                fieldTypes.put(fieldName, Type.INTEGER);
            } else if (fieldValue instanceof Long) {
                fieldTypes.put(fieldName, Type.INTEGER);
            } else if (fieldValue instanceof Float) {
                fieldTypes.put(fieldName, Type.DOUBLE);
            } else if (fieldValue instanceof Double) {
                fieldTypes.put(fieldName, Type.DOUBLE);
            } else if (fieldValue instanceof LocalDate) {
                fieldTypes.put(fieldName, Type.DATE);
            } else if (fieldValue instanceof LocalDateTime) {
                fieldTypes.put(fieldName, Type.DATETIME);
            } else if (fieldValue instanceof LocalTime) {
                fieldTypes.put(fieldName, Type.DURATION);
            } else {
                fieldTypes.put(fieldName, Type.UNICODE_STRING);
            }
        }

        return fieldMapCombiner(current, fieldTypes);
    }

    private static Map<String, Type> fieldMapCombiner(Map<String, Type> current, Map<String, Type> fieldTypes) {
        Map<String, Type> fieldMap = new HashMap<>(current);

        for (String fieldName : fieldTypes.keySet()) {
            Type fieldType = fieldTypes.get(fieldName);

            if (!fieldMap.containsKey(fieldName)) {
                fieldMap.put(fieldName, fieldType);
                continue;
            }

            if (fieldMap.get(fieldName) == null && fieldType != null) {
                fieldMap.replace(fieldName, fieldType);
            } else if (fieldType == Type.BOOLEAN) {
                fieldMap.merge(fieldName, Type.BOOLEAN, (oldValue, newValue) -> oldValue == newValue ? oldValue : Type.UNICODE_STRING);
            } else if (fieldType == Type.INTEGER) {
                fieldMap.merge(fieldName, Type.INTEGER, (oldValue, newValue) -> oldValue == newValue ? oldValue : (oldValue == Type.DOUBLE ? oldValue : Type.UNICODE_STRING));
            } else if (fieldType == Type.DOUBLE) {
                fieldMap.merge(fieldName, Type.DOUBLE, (oldValue, newValue) -> oldValue == newValue ? oldValue : (oldValue == Type.INTEGER ? newValue : Type.UNICODE_STRING));
            } else if (fieldType == Type.DATE) {
                fieldMap.merge(fieldName, Type.DATE, (oldValue, newValue) -> oldValue == newValue ? oldValue : (oldValue == Type.DATETIME ? oldValue : Type.UNICODE_STRING));
            } else if (fieldType == Type.DATETIME) {
                fieldMap.merge(fieldName, Type.DATETIME, (oldValue, newValue) -> oldValue == newValue ? oldValue : (oldValue == Type.DATE ? newValue : Type.UNICODE_STRING));
            } else if (fieldType == Type.DURATION) {
                fieldMap.merge(fieldName, Type.DURATION, (oldValue, newValue) -> oldValue == newValue ? oldValue : Type.UNICODE_STRING);
            } else {
                fieldMap.merge(fieldName, Type.UNICODE_STRING, (oldValue, newValue) -> Type.UNICODE_STRING);
            }
        }

        return fieldMap;
    }

    public Map<String, Type> getFieldMap() {
        return fieldMap;
    }
}
