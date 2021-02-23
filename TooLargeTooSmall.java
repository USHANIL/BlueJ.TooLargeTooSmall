//Field class
package com.jpmc.cto.ecdp;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Field {

    public String name;
    //private String uid;
    public String declaredTechnicalType;
    private boolean nullable;

   }


//SchemaGenerator

package com.jpmc.cto.ecdp;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class SchemaGenerator {

    public static void main(String args[]) {
        //String parquetFilePath = "I:\\Ramana\\work\\CIB\\datasets\\marketperson";
        String[] fileNames;
        File pathName = new File("C:\\test\\input");
        fileNames = pathName.list();

        for (String fileName : fileNames) {
            String parquetFilePath = "C:\\test\\input\\" + fileName;
            String justFileName = FilenameUtils.getBaseName(fileName);
            String jsonFilePath = "C:\\test\\output\\"  + justFileName + ".json";

            //Format the json string according to the format required for Databook Registration (start and ending json format)
            String jsonForRegistration1 = "{\n" +
                    "  \"name\": \"CFMInput\",\n" +
                    "  \"description\": \"FDL Model\",\n" +
                    "  \"applicationId\": \"89055\",\n" +
                    "  \"application\": {\"applicationId\":\"89055\"},\n" +
                    "  \"modelSourceType\": \"Parquet\",\n" +
                    "  \"schemas\": [\n" +
                    "\t{\n" +
                    "\t\t\"name\": \"" +
                    justFileName +
                    "\",\n" +
                    "\t\t\"description\": \"" +
                    justFileName +
                    "\",\n" +
                    "\t\t\"dataStructures\": [{\n" +
                    "\t\t\t\"name\": \"" +
                    justFileName +
                    "\",\n" +
                    "\t\t\t\"description\": \"" +
                    justFileName + " Description" +
                    "\",\n" +
                    "\t\t\t\"fields\":" +
                    "\n\t\t";
            String jsonForRegistration2 = "\n}]\n" +
                    "\t\t}]\n" +
                    "\t}]\n" +
                    "}" ;
            //Read parquet file and get the schema
            SparkSession sparkSession = SparkSession.builder()
                    .appName("test")
                    .master("local")
                    .getOrCreate();
            StructType parquetFile = sparkSession.read().parquet(parquetFilePath).schema();

            //Get the struct datatype fields frm the parquet schema to build nested datastructure
            List<Field> nestedFields = Arrays.stream(parquetFile.fields())
                    .map(
                            field -> Field.builder()
                            .name(field.name())
                            .declaredTechnicalType(field.dataType().json())
                            //.withColumn("declaredTechnicalType(field.dataType().json().toString())",)
                            // .declaredTechnicalType(field.dataType().typeName().toUpperCase())
                            .nullable(field.nullable())
                            .build()
                    )
                    .filter(ele -> ele.declaredTechnicalType.contains("struct"))
                    .collect(Collectors.toList());

                //Remove the struct data type from fields as it should be available as part of nested data structure
                List<Field> fields = Arrays.stream(parquetFile.fields())
                    .map(
                            field -> Field.builder()
                                    .name(field.name())
                                    // .nullable("false")
                                    //.withColumn("declaredTechnicalType(field.dataType().json().toString())"
                                    .declaredTechnicalType(field.dataType().typeName().toUpperCase())
                                    .nullable(field.nullable())
                                    .build()
                    )
                    .filter(ele1 -> !ele1.declaredTechnicalType.contains("STRUCT"))
                    .collect(Collectors.toList());


            //Print out the nested fields in a text file and can be used to manually copy it as nested data structure in the corresponding json
            try {
                if (nestedFields.size() > 0) {
                    String nestedFilePath = "C:\\test\\output\\" + justFileName + ".txt";
                    FileWriter fw = new FileWriter(nestedFilePath);
                    fw.write(justFileName + "\n");
                    for (Field element : nestedFields) {

                        String nestedFieldsList = element.declaredTechnicalType.split(Pattern.quote("["))[1].split(Pattern.quote("]"))[0];
                        String nestedFieldName = element.name;
                        fw.write(nestedFieldName + "\n");
                        fw.write(nestedFieldsList);
                        fw.write("\n\n");
                        /*
                        List<Field> nestedFieldsArray = Arrays.stream(gson2.toJson()
                           .map(
                                    field -> Field.builder()
                                            .name(field.name())
                                            .declaredTechnicalType(field.dataType().json())
                                            //.withColumn("declaredTechnicalType(field.dataType().json().toString())",)
                                            // .declaredTechnicalType(field.dataType().typeName().toUpperCase())
                                            .nullable(field.nullable())
                                            .build()
                            )
                            .collect(Collectors.toList()); */
                    }
                    fw.close();
                }
            }
            catch (Exception e) {
                System.out.println(e);
            }

            Gson gson1 = new GsonBuilder().setPrettyPrinting().create();
            try {
                FileWriter fw = new FileWriter(jsonFilePath);
                fw.write(jsonForRegistration1);
                gson1.toJson(fields, fw);
                fw.write(jsonForRegistration2);
               // System.out.println("jsonfile created");

               // JSONWriter jw = new JSONWriter(jsonFilePath);
                fw.close();
            } catch (Exception e) {
                System.out.println(e);
            }

        }
    }
}

// Pom.xml

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.jpmc.cto.ecdp</groupId>
    <artifactId>ecdp-schema-utils</artifactId>
    <version>1.0-SNAPSHOT</version>
    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <configuration>
                    <source>8</source>
                    <target>8</target>
                </configuration>
            </plugin>
        </plugins>
    </build>

    <properties>
        <lombok.version>1.18.8</lombok.version>
    </properties>

    <dependencies>
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql_2.11</artifactId>
            <version>2.4.4</version>
        </dependency>
        <dependency>
            <groupId>org.projectlombok</groupId>
            <artifactId>lombok</artifactId>
            <version>${lombok.version}</version>
        </dependency>
    </dependencies>



</project>


//settings.xml file in C:\Users\I739937\.m2
<settings xmlns="http://maven.apache.org/settings/1.0.0" 
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
	
  <proxies> 
	 <proxy>
      <active>true</active>
      <protocol>http</protocol>
      <host>approxy.jpmchase.net</host>
      <port>8080</port>
     </proxy>
  </proxies>
  <mirrors>
    <!-- mirror
     | Specifies a repository mirror site to use instead of a given repository. The repository that
     | this mirror serves has an ID that matches the mirrorOf element of this mirror. IDs are used
     | for inheritance and direct lookup purposes, and must be unique across the set of mirrors.
     |-->
    <mirror>
      <!--This sends everything else to /jpmc-public -->
      <id>jpmc-public</id>
      <!--mirrorOf>*,!FRSTEST</mirrorOf-->
	  <mirrorOf>*</mirrorOf>
      <url>http://repo-proxy.jpmchase.net/maven/content/groups/jpmc-public/</url>
    </mirror>
  </mirrors>

  <profiles>
     <profile>
       <id>frs</id>
       <!-- Enable jpmc snapshots repositories for the built in jpmc-public repository group to direct -->
       <!-- all requests to FRS via the mirror -->
       <repositories>
         <repository>
           <id>jpmc-public</id>
           <url>http://repo-proxy.jpmchase.net/maven/content/groups/jpmc-public/</url>
           <releases><enabled>true</enabled></releases>
           <snapshots><enabled>true</enabled></snapshots>
         </repository>
		 <repository>
			<id>cdh.repo</id>
			<url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
			<name>Cloudera Repositories</name>
			<snapshots>
				<enabled>false</enabled>
			</snapshots>
		</repository>
         <!--repository>
           <id>FRSTEST</id>
           <url>https://repo.jpmchase.net/maven/content/repositories/FRSTEST/</url>
           <releases><enabled>true</enabled></releases>
           <snapshots><enabled>false</enabled></snapshots>
         </repository-->
       </repositories>
      <pluginRepositories>
         <pluginRepository>
           <id>jpmc-public</id>
           <url>http://repo-proxy.jpmchase.net/maven/content/groups/jpmc-public/</url>
           <releases><enabled>true</enabled></releases>
           <snapshots><enabled>true</enabled></snapshots>
         </pluginRepository>
         <!--pluginRepository>
           <id>FRSTEST</id>
           <url>https://repo.jpmchase.net/maven/content/repositories/FRSTEST/</url>
           <releases><enabled>true</enabled></releases>
           <snapshots><enabled>false</enabled></snapshots>
         </pluginRepository-->
       </pluginRepositories>
     </profile>
   </profiles>
   
   <activeProfiles>
     <!-- make the profile active all the time -->
     <activeProfile>frs</activeProfile>
  </activeProfiles>

  
  <pluginGroups>
    <pluginGroup>org.sonatype.plugins</pluginGroup>
  </pluginGroups>

</settings>
