package com.jdunkerley;

import com.tableausoftware.TableauException;
import com.tableausoftware.common.Type;
import com.tableausoftware.extract.*;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MakeTDEFromJSON {

    private static final String EXTRACT = "Extract";

    public static void main(String[] args) {
        if (checkArguments(args)) return;

        JSONArray jsonArray = readJSONFile(args[0]);
        if (jsonArray == null) return;
        List<JSONObject> values = new ArrayList<>(jsonArray);

        Map<String, String> fieldMap = getFieldMap(values);

        MoveExtract(args[1]);

        try {
            ExtractAPI.initialize();
        } catch (TableauException ex) {
            System.out.println("Exception from Tableau:" + ex.getMessage());
            System.exit(-1);
        }

        try (Extract extract = new Extract(args[1])){
            TableDefinition definition = createTableDefinition(fieldMap);
            if (definition == null) {
                return;
            }

            Table table = extract.addTable(EXTRACT, definition);
            values.forEach(j -> AddRowToTable(table, definition, fieldMap, j));

            definition.close();
        } catch (TableauException ex) {
            System.out.println("Exception from Tableau:" + ex.getMessage());
            System.exit(-1);
        }
    }

    private static void MoveExtract(String fileName) {
        File f = new File(fileName);
        if (f.exists()) {
            try {
                BasicFileAttributes attr = Files.readAttributes(f.toPath(), BasicFileAttributes.class);
                String isoDate = attr.creationTime().toString().substring(0, 10).replaceAll("-", "");

                File d = new File(f.getParent(), f.getName().replace(".tde", "." + isoDate + ".tde"));
                int count = 0;
                while (d.exists()) {
                    count++;
                    d = new File(f.getParent(), f.getName().replace(".tde", "." + isoDate + "_" + count + ".tde"));
                }

                f.renameTo(d);
            } catch (IOException ex) {
                System.out.println("Error moving existing extract:" + ex.getMessage());
            }
        }
    }

    private static Map<String, String> getFieldMap(List<JSONObject> values) {
        Set<String> jsonKeys = values
                .stream()
                .flatMap(j -> new ArrayList<String>(j.keySet()).stream())
                .collect(Collectors.toSet());
        return jsonKeys.stream().collect(Collectors.toMap(MakeTDEFromJSON::jsonFieldToTableau, Function.identity()));
    }

    private static void AddRowToTable(Table table, TableDefinition definition, Map<String, String> fieldMap, JSONObject jsonObject) {
        if (jsonObject == null) return;

        try {
            Row row = new Row(definition);

            for (int i = 0; i < definition.getColumnCount(); i++) {
                String columnName = definition.getColumnName(i);
                Type columnType = definition.getColumnType(i);
                String jsonField = fieldMap.get(columnName);

                if (!jsonObject.containsKey(jsonField)) continue;

                Object jsonValue = jsonObject.get(jsonField);

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
                        }
                        break;
                    case CHAR_STRING:
                        row.setCharString(i, jsonValue.toString());
                        break;
                    case DATE:
                        String jsonText = jsonValue.toString();
                        if (jsonText.matches("\\d{4}-\\d{2}-\\d{2}")) {
                            row.setDate(i, Integer.parseInt(jsonText.substring(0,4)), Integer.parseInt(jsonText.substring(5,7)), Integer.parseInt(jsonText.substring(8,10)));
                        }
                        break;
                }
            }

            row.close();
            table.insert(row);
        } catch (TableauException ex) {
            System.out.println("Error writing row: " + jsonObject.toJSONString());
        }
    }

    private static TableDefinition createTableDefinition(Map<String, String> fieldMap) {
        try {
            TableDefinition definition = new TableDefinition();

            for (String tableauField : fieldMap.keySet()) {
                definition.addColumn(tableauField, getColumnType(tableauField));
            }

            return definition;
        } catch (TableauException ex) {
            System.out.println("Error creating definition: " + ex.getMessage());
            System.exit(-1);
            return null;
        }
    }

    private static Type getColumnType(String tableauField) {
        if (tableauField.equals("Hired")) {
            return Type.BOOLEAN;
        }

        if (tableauField.matches(".*Date")) {
            return  Type.DATE;
        }

        if (tableauField.matches("Score .*|.* Id|.* Count")) {
            return Type.INTEGER;
        }

        return Type.CHAR_STRING;
    }

    private static String jsonFieldToTableau(String jsonField) {
        return Arrays.stream(jsonField.split("(?=[A-Z])"))
                .map(k -> " " + k.substring(0,1).toUpperCase() + k.substring(1))
                .collect(Collectors.joining())
                .trim();
    }

    private static JSONArray readJSONFile(String fileName) {
        JSONParser parser = new JSONParser();

        try {
            Object o = parser.parse(new FileReader(fileName));
            return (JSONArray)o;
        } catch (FileNotFoundException fileException) {
            System.out.println("Unable to find JSON file:" + fileName + " (" + fileException.getMessage() + ")");
            System.exit(-1);
            return null;
        } catch (IOException ioException) {
            System.out.println("Unable to read file:" + ioException.getMessage());
            System.exit(-1);
            return null;
        } catch (ParseException parseException){
            System.out.println("Unable to parse JSON:" + parseException.getMessage());
            System.exit(-1);
            return null;
        }
    }

    private static boolean checkArguments(String[] args) {
        if (args.length != 2) {
            System.out.println("Syntax: <JSONFIle> <TDEOutput>");
            System.exit(-1);
            return true;
        }
        return false;
    }
}
