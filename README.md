# tableauExtractFromJSON

The goal of this project is given a JSON file containing an array of objects create a Tableau Hyper Extract based upon it. The column types will derived from the JSON content. 

Currently it only support a flat structure (i.e. no child objects or child arrays). the input need to look something like:

```javascript
[
    { "A": true },
    { "B": 123.5 },
    { "A": true, "C": "2018-01-21" }
]
```

producing:

|A|B|C|
|---|---|---|
|true|*null*|*null*|
|*null*|123.5|*null*|
|true|*null*|21/1/18|

In order to run (or build) the project, you will need to install the Tableau Extract API. The API instllation guide is at https://onlinehelp.tableau.com/v10.5/api/extract_api/en-us/help.htm#Extract/extract_api_installing.htm. Download and extract the C/C++/Java version for your OS to a location you want to run it from.

In addition the last step of the Java instructions (adding the bin folder to path) https://onlinehelp.tableau.com/v10.5/api/extract_api/en-us/help.htm#Extract/extract_api_using_java_eclipse.htm needs to be followed so Java can find the Hyper DLLs.

This project uses Java Streams, so Java 1.8 or later runtime will be needed. In addition as the Tableau API is 64-bit then it will need to be on a 64-bit run time. 

In order to build the project, you will need the Java SDK (1.8 or later, with JAVA_HOME set to this if using the command line to build) and [Maven](https://maven.apache.org/) installed.

## Command Syntax

```text
java -jar tableauExtractFromJSON-jar-with-dependencies.jar [HyperFileName]
```

The only required argument is a path to the JSON file. By default, if not specified, the hyper extract will just be called the same as the json file but with a `.hyper` extract.

## Building the JAR file

The extract API is not published to Maven Central so you will need to download from Tableau and add to your local maven repository. Within the Extract API, there is a Java folder containing 3 jar files. The two tableau ones (i.e. not the jni.jar one) need to be added to you maven local repository for this project to build:

```bash
mvn install:install-file -Dfile=tableaucommon.jar -DgroupId=com.tableausoftware -DartifactId=tableau-common -Dversion=10.5.0 -Dpackaging=jar
mvn install:install-file -Dfile=tableauhyperextract.jar -DgroupId=com.tableausoftware -DartifactId=tableau-hyper-extract -Dversion=10.5.0 -Dpackaging=jar
```

Now, you should be able to build with (running the commands in the root directory of this project):

```bash
mvn clean compile assembly:single
```

There should be a jar file built into the `target` subfolder called `tableauExtractFromJSON-jar-with-dependencies.jar`. After you have built it you can run:

```bash
java -jar target\tableauExtractFromJSON-2.0-jar-with-dependencies.jar test.json
```

This should produce a simple Hyper extract with the data above.

## The Process

The program uses the [Jackson-Core](https://github.com/FasterXML/jackson-core) library to read the JSON array as a stream. The file is read through twice. The first time is to establish the list of fields and their type to build the table definition. The second pass is to actually load the JSON data to the hyper extract.

The main class `MakeTDEFromJSON` is the main executable sequence of the program. It does the following:

- Validate the arguments
- Create the field map
- Move any existing extract out the way
- Initialise the ExtractAPI
- Create the TableDefinition from the field map
- Load the data to the Extract
- Clean up after running

### Streaming the JSON

The first class of this project `JSONObjectSupplier` is a wrapper for the Jackson-Core JsonParser which reads a file in and creates a `Spliterator<Map<String, Object>>`. The file is read object by object so the memory utilisation should be kept low. 

If the file is not a simple JSON array of primitve values then `IOException` will be thrown and stored in an `Exception` property on this class. In addition any other `IOException` when reading the JSON will be stored on this property. Once there has been an issue, the stream will not continue to proceed.

This class is also responsible for parsing dates, datetimes and times stored as strings in the JSON into respective Java classes. Due to issues with storing Durations in the Hyper extract I have added a flag to switch off the time parsing. Currently the format supported are:

- 2018-01-31 ==> LocalDate
- 2018-01-31T20:06:15+00:00 ==> LocalDateTime
- 2018-01-31T20:06:15Z ==> LocalDateTime
- 13:45 ==> LocalTime
- 13:45:13 ==> LocalTime

If a string is encountered which matches the pattern but which cannot be parsed into a the Java class a warning message will be added to `ParseWarnings` property on the class. These warnings will not stop the stream from proceeding but will result in the column being treated as strings.

Due to the methods available on the Tableau ExtractAPI, I chose to store integers in the JSON as longs and floats in the JSON as doubles.

### Building the Field Map

The second class in this project `TableauFieldMap` takes a `Stream<Map<String, Object>>` as an input and builds up a unioned collection of all the fields and compute the appropriate type for the column in Tableau. As the column may not be one single type there is some basic rules used to build it up. When the types cannot be combined then the column will default to a `UNICODE_STRING` String. The rules are roughly:

- All values are `null`, then the column will be `BOOLEAN`
- If the whole column are `Boolean` objects or `null`, then `BOOLEAN`
- If the whole column is a mix of `Long`, `Integer`, `Short` and `Byte` objects or `null`, then `INTEGER`
- If the whole column is a mix of `Double`, `Float`, `Long`, `Integer`, `Short` and `Byte` objects or `null`, then `Double`
- If the whole column are `LocalDate` objects or `null`, then `DATE`
- If the whole column are `LocalDate` and `LocalDateTime` objects or `null`, then `DATETIME`
- If the whole column are `LocalTime` objects or `null`, then `TIME`
- Otherwise, a `UNICODE_STRING` will be used

For simplicity, I always used `UNICODE_STRING` and never used `CHAR_STRING`.

I haven't added `BigInteger` or `BigDecimal` support to the type calculation as these could exceed the allowed range on the Tableau methods.

The result of the field map process is a `Map<String,Type>`

### Creating the Extract

The first task when working with the Extract API is to set it up:

```java
System.out.println("Initialize Extract API...");
try {
ExtractAPI.initialize();
} catch (TableauException ex) {
System.err.println("Exception from Tableau During Init:" + ex.getMessage());
ex.printStackTrace(System.err);
System.exit(-1);
}
```

Next, we take the field map and convert to a `TableDefinition`:

```java
TableDefinition definition = new TableDefinition();
definition.setDefaultCollation( Collation.EN_GB );

for (String tableauField : fieldMap.keySet()) {
Type columnType = fieldMap.get(tableauField);
if (columnType == null) {
columnType = Type.BOOLEAN;
}

definition.addColumn(tableauField, columnType);
}
```

This is then used to create the table in the extract, which must be called `Extract`:

```java
Table table = extract.addTable(EXTRACT, definition);
```

Next the JSON file stream is iterated over again. For each object in the stream, the columns of the table are then iterated over. If the `Map<String, Object>` representing the JSON row contains the property then value is used to set a value on a `Row` in the Extract. Something like:

```java
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
// Truncated for docs
}
}

table.insert(row);
row.close();
```

After this the table will be populted with data.. **I have currently had some difficulties populating `DURATION` columns. It seems to cause Tableau 10.5 to be unhappy.**.

Finally just clean up:

```java
definition.close();
extract.close();
ExtractAPI.cleanup();
```
