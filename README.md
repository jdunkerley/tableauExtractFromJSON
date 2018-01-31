# tableauExtractFromJSON

The goal of this project is given a JSON file containing an array of objects create a Tableau Hyper Extract based upon it. The column types will derived from the JSON content. Currently it only support a flat structure (i.e. no child objects or child arrays). 

In order to build the project you will need to install the Extract API.

This project uses Java Streams for a Java 1.8 or later runtime will be needed. In addition as the Tableau API is 64-bit then it will need to be on a 64-bit run time.

## Command Syntax

```text
java -jar tableauExtractFromJSON <JSONFileName> [HyperFileName]
```

The only required argument is a path to the JSON file. By default, if not specified, the hyper extract will just be called the same as the json file but with a `.hyper` extract.

## Installing the Extract API

The extract API is not published to Maven Central so you will need to download from Tableau and add to your local maven.

The API instllation guide is at https://onlinehelp.tableau.com/v10.5/api/extract_api/en-us/help.htm#Extract/extract_api_installing.htm. Download and extract the C/C++/Java version for your OS to a location you want to run it from.

Within the extract there is a Java folder containing 3 jar files. The two tableau ones need to be added to you maven local repository for this project to build:

```bash
mvn install:install-file -Dfile=tableaucommon.jar -DgroupId=com.tableausoftware -DartifactId=tableau-common -Dversion=10.5.0 -Dpackaging=jar
mvn install:install-file -Dfile=tableauhyperextract.jar -DgroupId=com.tableausoftware -DartifactId=tableau-hyper-extract -Dversion=10.5.0 -Dpackaging=jar
```

In addition the last step of the Java instructions (adding the bin folder to path) https://onlinehelp.tableau.com/v10.5/api/extract_api/en-us/help.htm#Extract/extract_api_using_java_eclipse.htm needs to be followed.

## Building the JAR

