package com.jdunkerley;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.function.Consumer;

public class JSONObjectSupplier implements Spliterator<Map<String, Object>> {
    private static boolean parseTimes;
    private static final JsonFactory jsonFactory = new JsonFactory();

    public static void ParseTimesFeature(boolean enabled) {
        parseTimes = enabled;
    }

    private JsonParser jsonParser;
    private List<String> parseWarnings;
    private IOException exception;

    JSONObjectSupplier(String fileName) {
        this.parseWarnings = new ArrayList<>();
        try {
            this.jsonParser = jsonFactory.createParser(new File(fileName));

            if (this.jsonParser.nextToken() != JsonToken.START_ARRAY) {
                this.exception = new IOException("Unexpected JSON format");
                this.jsonParser.close();
            }

            this.jsonParser.nextToken();
        } catch (IOException fileException) {
            this.exception = fileException;
        }
    }

    @Override
    public boolean tryAdvance(Consumer<? super Map<String, Object>> action) {
        if (this.exception != null || this.jsonParser == null || this.jsonParser.isClosed())
            return false;

        try {
            if (this.jsonParser.currentToken() == JsonToken.START_OBJECT) {
                Map<String, Object> objectMap = LoadObjectToMap(this.jsonParser);
                action.accept(objectMap);
                this.jsonParser.nextToken();
                return true;
            }

            if (this.jsonParser.currentToken() != JsonToken.END_ARRAY) {
                this.exception = new IOException("Unexpected JSON format");
            }

            this.jsonParser.close();
            return false;
        } catch (IOException fileException) {
            this.exception = fileException;
            return false;
        }
    }

    public IOException getException() {
        return this.exception;
    }

    public List<String> getParseWarnings() {
        return this.parseWarnings;
    }

    @Override
    public Spliterator<Map<String, Object>> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return 0;
    }

    @Override
    public int characteristics() {
        return 0;
    }

    private Map<String, Object> LoadObjectToMap(JsonParser p) throws IOException {
        Map<String, Object> output = new HashMap<>();

        while (p.nextToken() != JsonToken.END_OBJECT) {
            if (p.currentToken() == JsonToken.FIELD_NAME) {
                String fieldName = p.getValueAsString();
                JsonToken fieldValue = p.nextToken();
                if (fieldValue == JsonToken.VALUE_TRUE) {
                    output.put(fieldName, Boolean.TRUE);
                } else if (fieldValue == JsonToken.VALUE_FALSE) {
                    output.put(fieldName, Boolean.FALSE);
                } else if (fieldValue == JsonToken.VALUE_NUMBER_INT) {
                    output.put(fieldName, p.getLongValue());
                } else if (fieldValue == JsonToken.VALUE_NUMBER_FLOAT) {
                    output.put(fieldName, p.getDecimalValue());
                } else if (fieldValue == JsonToken.VALUE_STRING) {
                    String jsonString = p.getValueAsString();

                    LocalDate localDate = ParseDate(jsonString);
                    if (localDate != null) {
                        output.put(fieldName, localDate);
                        continue;
                    }

                    LocalDateTime localDateTime = ParseDateTime(jsonString);
                    if (localDateTime != null) {
                        output.put(fieldName, localDateTime);
                        continue;
                    }

                    if (parseTimes) {
                        LocalTime localTime = ParseTime(jsonString);
                        if (localTime != null) {
                            output.put(fieldName, localTime);
                            continue;
                        }
                    }

                    output.put(fieldName, jsonString);
                } else if (fieldValue != JsonToken.VALUE_NULL) {
                    // Could add Nested Object or Array Support
                    throw new IOException("Unsupported object structure...");
                }
            }
        }

        return output;
    }

    private LocalDate ParseDate(String jsonText) {
        if (jsonText.matches("\\d{4}-\\d{2}-\\d{2}")) {
            try {
                return LocalDate.parse(jsonText, DateTimeFormatter.ISO_DATE);
            } catch (DateTimeParseException e) {
                this.parseWarnings.add("Date Parse Error (" + jsonText + ") - " + e.getMessage());
            }
        }

        return null;
    }

    private LocalDateTime ParseDateTime(String jsonText) {
        if (jsonText.matches("\\d{4}-\\d{2}-\\d{2}T\\d+.*")) {
            try {
                return LocalDateTime.parse(jsonText, DateTimeFormatter.ISO_DATE);
            } catch (DateTimeParseException e) {
                this.parseWarnings.add("DateTime Parse Error (" + jsonText + ") - " + e.getMessage());
            }
        }

        return null;
    }

    private LocalTime ParseTime(String jsonText) {
        if (jsonText.matches("\\d{2}:\\d{2}") || jsonText.matches("\\d{2}:\\d{2}:\\d{2}")) {
            try {
                return LocalTime.parse(jsonText, DateTimeFormatter.ISO_LOCAL_TIME);
            } catch (DateTimeParseException e) {
                this.parseWarnings.add("Time Parse Error (" + jsonText + ") - " + e.getMessage());
            }
        }

        return null;
    }
}
