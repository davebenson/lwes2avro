package com.openx.lwes2avro;

import org.lwes.db.EventTemplateDB;

public class LWES2AVRO {

  private static String primitiveTypeAsString(FieldType type) {
    switch (type) {
      case UINT16: return "int";
      case INT16: return "int";
      case UINT32: return "int";
      case INT32: return "int";
      case STRING: return "string";
      case IPADDR: return "bytes";
      case INT64: return "long";
      case UINT64: return "long";
      case BOOLEAN: return "boolean";
      case BYTE: return "int"; // Bytes are considered unsigned and range from 0..255.
      case FLOAT: return "float";
      case DOUBLE: return "double";
      default: throw new RuntimeException("bad primitive-type: " + type);
    }
  }
  public static void main(String[] args) {
    String dbfile = args[0];
    String outdir = args[1];
    String esfAvroNamespace = "com.openx";

    //
    // Parse and initialize.
    //
    EventTemplateDB db = new EventTemplateDB();
    db.setESFFile(dbfile);
    db.initialize();

    //
    // Generate avro schema.
    //
    for (Map.Entry<String, Map<String, BaseType>> entry : db.getEvents()) {
      String eventName = entry.getKey();
      Map<String, BaseType> fields = entry.getValue();
      PrintWriter pr = outdir + "/" + eventName + ".avsc";
      pr.print("{\n  \"type\":\"record\",\n"
                  "  \"name\":\"" + eventName + "\",\n"
                  "  \"namespace\":\"com.openx\",\n"
                  "  \"fields\":[\n");
      for (Map.Entry<String, BaseType> field : fields) {
        String fieldName = field.getKey();
        BaseType fieldBaseType = field.getValue();
        FieldType fieldType = fieldBaseType.getType();
        FieldType primitiveType = FieldType.byToken(fieldType.token & 0x0f);
        String primTypeAsString = primitiveTypeAsString(primitiveType);
        if ((fieldType.token & 0x80) == 0) {
          // classic primitive types
          fieldTypeAsJsonString = "\"" + primTypeAsString + "\"";
        } else if ((fieldType.token & 0x40) == 0) {
          // arrays of classic primitive types
          fieldTypeAsJsonString = "{\"type\":\"array\",\"items\":\""
                                + primTypeAsString
                                + "\"}";
        } else {
          // sparse arrays of classic primitive types
          //
          // we match the APIs which tend to provide:
          //
          //        a map from an index -> the primitive value,
          //
          // but actually the storage format uses a bitmap, which
          // is more suitable for "dense" sparse arrays.
          // <shrug> we are interested in compatbility, not efficiency here
          //
          // So a nullable-MyType-array has type:
          //    {{record, [index,string],[value,MyType]}}  //XXX
          fieldTypeAsJsonString = "{\"type\":\"array\","
                                + "\"items\":{\"type\":\"record\":"
                                +           "\"fields\":"
                                +              "[{\"type\":\"int\",\"name\":\"index\"},"
                                +              "{\"type\":\""+primTypeAsString+"\","
                                +               " \"name\":\"value\"}]}"
                                + "\"}";
        }
        if (!isFirst) {
          pr.print(",\n");
          isFirst = false;
        }
        pr.print("  { \"name\":\"" + fieldName
                  + ",\"type\":" + fieldTypeAsJsonString + "}");
      }
      pr.print("  ]\n}\n");

      //
      // Part 2. Emit org.lwes.Event -> avro binary-data code.
      //
      ...
    }
  }
}
