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
