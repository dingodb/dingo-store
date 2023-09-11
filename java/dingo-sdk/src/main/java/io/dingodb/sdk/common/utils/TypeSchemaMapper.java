package io.dingodb.sdk.common.utils;

import io.dingodb.sdk.common.serial.schema.DingoSchema;
import io.dingodb.sdk.common.serial.schema.*;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
public class TypeSchemaMapper {
   private static final Map<String, Supplier<DingoSchema>> typeSchemaMap;
   static {
      typeSchemaMap = new HashMap<>();
      // add scala type mapping
      addScalaTypeSchemaMapping("INT", IntegerSchema::new);
      addScalaTypeSchemaMapping("INTEGER", IntegerSchema::new);
      addScalaTypeSchemaMapping("TINYINT", IntegerSchema::new);
      addScalaTypeSchemaMapping("LONG", LongSchema::new);
      addScalaTypeSchemaMapping("BIGINT", LongSchema::new);
      addScalaTypeSchemaMapping("BOOL", BooleanSchema::new);
      addScalaTypeSchemaMapping("BOOLEAN", BooleanSchema::new);
      addScalaTypeSchemaMapping("FLOAT", FloatSchema::new);
      addScalaTypeSchemaMapping("DOUBLE", DoubleSchema::new);
      addScalaTypeSchemaMapping("REAL", DoubleSchema::new);
      addScalaTypeSchemaMapping("DECIMAL", StringSchema::new);
      addScalaTypeSchemaMapping("STRING", StringSchema::new);
      addScalaTypeSchemaMapping("CHAR", StringSchema::new);
      addScalaTypeSchemaMapping("VARCHAR", StringSchema::new);
      addScalaTypeSchemaMapping("DATE", LongSchema::new);
      addScalaTypeSchemaMapping("TIME", LongSchema::new);
      addScalaTypeSchemaMapping("TIMESTAMP", LongSchema::new);
      addScalaTypeSchemaMapping("VECTOR", VectorSchema::new);
      addScalaTypeSchemaMapping("BINARY", BytesSchema::new);
      addScalaTypeSchemaMapping("BYTES", BytesSchema::new);
      addScalaTypeSchemaMapping("VARBINARY", BytesSchema::new);
      addScalaTypeSchemaMapping("BLOB", BytesSchema::new);
      addScalaTypeSchemaMapping("MAP", BytesSchema::new);
      addScalaTypeSchemaMapping("TUPLE", BytesSchema::new);
      addScalaTypeSchemaMapping("DICT", BytesSchema::new);
      addScalaTypeSchemaMapping("OBJECT", BytesSchema::new);
      addScalaTypeSchemaMapping("ANY", BytesSchema::new);

      // add array type mapping
      addCompoundTypeSchemaMapping("ARRAY","INT", IntegerListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "INTEGER", IntegerListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY","TINYINT", IntegerListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY","LONG", LongListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "BIGINT", LongListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "BOOL", BooleanListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "BOOLEAN", BooleanListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "FLOAT", FloatListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "DOUBLE", DoubleListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "REAL", DoubleListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "DECIMAL", StringListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "STRING", StringListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "CHAR", StringListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "VARCHAR", StringListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "DATE", LongListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "TIME", LongListSchema::new);
      addCompoundTypeSchemaMapping("ARRAY", "TIMESTAMP", LongListSchema::new);
   }

   public synchronized static void addScalaTypeSchemaMapping(String typeName, Supplier<DingoSchema> schemaSupplier) {
      if(typeSchemaMap.containsKey(typeName)) {
         throw new IllegalArgumentException("Adding existing "+ typeName +" types is not supported");
      }
      typeSchemaMap.put(typeName, schemaSupplier);
   }

   public synchronized static void addCompoundTypeSchemaMapping(String typeName, String elementType, Supplier<DingoSchema> schemaSupplier) {
      String key = getTypeName(typeName);
      String finalKey = getFinalKey(key, elementType);
      if(typeSchemaMap.containsKey(finalKey)) {
         throw new IllegalArgumentException("Adding existing "+ typeName +" types and elementType "+ elementType +" is not supported");
      }
      typeSchemaMap.put(finalKey, schemaSupplier);
   }
   
   public static DingoSchema getSchemaForTypeName(String typeName, String elementType) {
      String key = getTypeName(typeName);
      String finalKey = getFinalKey(key, elementType);
      Supplier<DingoSchema> schemaSupplier = typeSchemaMap.get(finalKey);
      if (schemaSupplier == null) {
         throw new IllegalArgumentException("There is no schema mapping for "+ typeName + " and " + elementType);
      }
      return schemaSupplier.get();
   }

   private static String getFinalKey(String typeName, String elementType) {
      if(elementType !=null && !elementType.equals("")) {
         return typeName + ":" + elementType.toUpperCase();
      }
      return typeName;
   }

   private static String getTypeName(String typeName) {
      String key = typeName.toUpperCase();
      if(key.equals("ARRAY") || key.equals("LIST") || key.equals("MULTISET")) {
         key = "ARRAY";
      }
      return key;
   }

   public static boolean checkType(String typeName, String elementType) {
      String key = getTypeName(typeName);
      String finalKey = getFinalKey(key, elementType);
      return typeSchemaMap.containsKey(finalKey);
   }
}