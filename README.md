# tableauExtractFromJSON
Given a JSON file containing an array of objects create a Tableau Extract


## Extract API

The extract API is not published to Maven Central so you will need to download from Tableau and add to your local maven.

The API is currently at `https://downloads.tableau.com/tssoftware/extractapi-x64-10-5-0.zip`

```bash
mvn install:install-file -Dfile=tableaucommon.jar -DgroupId=com.tableausoftware -DartifactId=tableau-common -Dversion=10.5.0 -Dpackaging=jar
mvn install:install-file -Dfile=tableauhyperextract.jar -DgroupId=com.tableausoftware -DartifactId=tableau-hyper-extract -Dversion=10.5.0 -Dpackaging=jar
```    
