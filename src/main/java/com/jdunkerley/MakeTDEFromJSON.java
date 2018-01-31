package com.jdunkerley;

import com.tableausoftware.TableauException;
import com.tableausoftware.common.*;
import com.tableausoftware.hyperextract.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.StreamSupport;

public class MakeTDEFromJSON {

    private static final String EXTRACT = "Extract";
    private static final String EXTENSION = ".hyper";

    public static void main(String[] args) {
        if (checkArguments(args)) return;
        String source = args[0];
        String target = args.length == 1 ? source + EXTENSION : args[1];

        System.out.println("Disabling Time to Duration ...");
        JSONObjectSupplier.ParseTimesFeature(false);

        System.out.println("Creating Field Map ...");
        Map<String, Type> fieldMap = getTableauFieldMap(source);
        if (fieldMap == null) {
            System.exit(-1);
        }

        MoveExtract(target);

        System.out.println("Initialize Extract API...");
        try {
            ExtractAPI.initialize();
        } catch (TableauException ex) {
            System.err.println("Exception from Tableau During Init:" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(-1);
        }

        try (Extract extract = new Extract(target)){
            System.out.println("Creating Table ...");
            TableDefinition definition = createTableDefinition(fieldMap);
            if (definition == null) {
                return;
            }
            Table table = extract.addTable(EXTRACT, definition);

            System.out.println("Loading Table Data ...");
            JSONObjectSupplier spliterator = new JSONObjectSupplier(source);
            StreamSupport.stream(spliterator, false)
                    .forEach(j -> AddRowToTable(table, definition, j));
            if (spliterator.getException() != null) {
                System.err.println("Error loading data: " + spliterator.getException().getMessage());
            }
            spliterator.getParseWarnings().forEach(System.out::println);

            System.out.println("Close Extract ...");
            definition.close();
            extract.close();
        } catch (TableauException ex) {
            System.err.println("Exception from Tableau:" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(-1);
        }

        System.out.println("Cleaning Up ...");
        try {
            ExtractAPI.cleanup();
        } catch (TableauException ex) {
            System.err.println("Exception from Tableau During Clean Up:" + ex.getMessage());
            ex.printStackTrace(System.err);
            System.exit(-1);
        }
    }

    private static Map<String, Type> getTableauFieldMap(String source) {
        JSONObjectSupplier spliterator = new JSONObjectSupplier(source);

        TableauFieldMap fieldMap = new TableauFieldMap(StreamSupport.stream(spliterator, false));

        if (spliterator.getException() != null) {
            System.err.println("Exception Creating Field Map: " + spliterator.getException().getMessage());
            return null;
        }

        return fieldMap.getFieldMap();
    }

    private static boolean checkArguments(String[] args) {
        if (args.length < 1) {
            System.out.println("Syntax: <JSONFIle> [HyperOutput]");
            System.exit(-1);
            return true;
        }
        return false;
    }

    private static void MoveExtract(String fileName) {
        File f = new File(fileName);
        if (f.exists()) {

            System.out.println("Moving Existng Hyper Extract...: " + fileName);

            try {
                BasicFileAttributes attr = Files.readAttributes(f.toPath(), BasicFileAttributes.class);
                String isoDate = attr.creationTime().toString().substring(0, 10).replaceAll("-", "");

                File d = new File(f.getParent(), f.getName().replace(EXTENSION, "." + isoDate + EXTENSION));
                int count = 0;
                while (d.exists()) {
                    count++;
                    d = new File(f.getParent(), f.getName().replace(EXTENSION, "." + isoDate + "_" + count + EXTENSION));
                }

                f.renameTo(d);
            } catch (IOException ex) {
                System.out.println("Error moving existing extract:" + ex.getMessage());
            }
        }
    }

    private static TableDefinition createTableDefinition(Map<String, Type> fieldMap) {
        try {
            TableDefinition definition = new TableDefinition();
            definition.setDefaultCollation( Collation.EN_GB );

            for (String tableauField : fieldMap.keySet()) {
                Type columnType = fieldMap.get(tableauField);
                if (columnType == null) {
                    columnType = Type.BOOLEAN;
                }

                definition.addColumn(tableauField, columnType);
            }

            return definition;
        } catch (TableauException ex) {
            System.out.println("Error creating definition: " + ex.getMessage());
            System.exit(-1);
            return null;
        }
    }

    private static void AddRowToTable(Table table, TableDefinition definition, Map<String, Object> jsonObject) {
        if (jsonObject == null) return;

        try {
            Row row = new Row(definition);

            for (int i = 0; i < definition.getColumnCount(); i++) {
                String columnName = definition.getColumnName(i);
                Type columnType = definition.getColumnType(i);

                if (!jsonObject.containsKey(columnName)) continue;
                Object jsonValue = jsonObject.get(columnName);

                row.setNull(i);
                if (jsonValue == null) {
                    continue;
                }

                switch (columnType) {
                    case BOOLEAN:
                        if (jsonValue instanceof Boolean) {
                            row.setBoolean(i, (Boolean)jsonValue);
                        }
                        break;
                    case INTEGER:
                        if (jsonValue instanceof Long) {
                            row.setLongInteger(i, (Long)jsonValue);
                        } else if (jsonValue instanceof Integer) {
                            row.setLongInteger(i, (Integer)jsonValue);
                        } else if (jsonValue instanceof Short) {
                            row.setLongInteger(i, (Short)jsonValue);
                        } else if (jsonValue instanceof Byte) {
                            row.setLongInteger(i, (Byte)jsonValue);
                        }
                        break;
                    case DOUBLE:
                        if (jsonValue instanceof Double) {
                            row.setDouble(i, (Double)jsonValue);
                        } else if (jsonValue instanceof Float) {
                            row.setDouble(i, (Float)jsonValue);
                        } else if (jsonValue instanceof Long) {
                            row.setLongInteger(i, (Long)jsonValue);
                        } else if (jsonValue instanceof Integer) {
                            row.setLongInteger(i, (Integer)jsonValue);
                        } else if (jsonValue instanceof Short) {
                            row.setLongInteger(i, (Short)jsonValue);
                        } else if (jsonValue instanceof Byte) {
                            row.setLongInteger(i, (Byte)jsonValue);
                        }
                        break;
                    case DATE:
                        if (jsonValue instanceof LocalDate) {
                            LocalDate date = (LocalDate)jsonValue;
                            row.setDate(i, date.getYear(), date.getMonth().getValue(), date.getDayOfMonth());
                        }
                        break;
                    case DATETIME:
                        if (jsonValue instanceof LocalDateTime) {
                            LocalDateTime dateTime = (LocalDateTime) jsonValue;
                            row.setDateTime(i, dateTime.getYear(), dateTime.getMonth().getValue(), dateTime.getDayOfMonth(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond(), 0);
                        } else if (jsonValue instanceof LocalDate) {
                            LocalDate date = (LocalDate)jsonValue;
                            row.setDateTime(i, date.getYear(), date.getMonth().getValue(), date.getDayOfMonth(), 0, 0, 0, 0);
                        }
                        break;
                    case DURATION:
                        if (jsonValue instanceof LocalTime) {
                            LocalTime time = (LocalTime)jsonValue;
                            row.setDuration(i, 0, time.getHour(), time.getMinute(), time.getSecond(), 0);
                        }
                        break;
                    case UNICODE_STRING:
                        row.setString(i, jsonValue.toString());
                        break;
                }
            }

            table.insert(row);
            row.close();
        } catch (TableauException ex) {
            System.out.println("Error writing row: " + jsonObject);
        }
    }
}
