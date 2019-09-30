package org.lwes.lwes2avro;

import org.lwes.FieldType;
import org.lwes.BaseType;
import org.lwes.db.EventTemplateDB;
import java.io.PrintWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

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


  static int baseToken(int token) {
    if (token < 16)
      return token;
    else if (token <= 0x80)
      throw new RuntimeException("base token");
    else if (token < 0x8d)
      return token - 0x80;
    else if (token < 0x99)
      return token - 0x8d + 1;
    else
      throw new RuntimeException("base token");
  }



  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("usage: LWES2AVRO DBFILE OUTPUT");
      System.exit(1);
    }
    String dbfile = args[0];
    String outdir = args[1];
    String esfAvroNamespace = "com.openx";

    //
    // Parse and initialize.
    //
    EventTemplateDB db = new EventTemplateDB();
    db.setESFFile(new File(dbfile));
    db.initialize();

    //
    // Generate avro schema.

    ArrayList<String> eventNames = new ArrayList<>();
    HashMap<String, ArrayList<String>> fieldsByEvent = new HashMap<>();
    for (Map.Entry<String, Map<String, BaseType>> entry : db.getEvents().entrySet()) {
      String eventName = entry.getKey();
      eventNames.add(eventName);
      System.err.println("event = " + eventName);
      Map<String, BaseType> fields = entry.getValue();
      ArrayList<String> fieldOrdering = new ArrayList<>();
      String escapedEventName = eventName.replace("::", "__");
      try (PrintWriter pr = new PrintWriter(outdir + "/" + escapedEventName + ".avsc")) {
        pr.print("{\n  \"type\":\"record\",\n"
                 +  "  \"name\":\"" + eventName + "\",\n"
                 +  "  \"namespace\":\"com.openx\",\n"
                 +  "  \"fields\":[\n");
        fieldsByEvent.put(eventName, fieldOrdering);
        boolean isFirst = true;
        for (Map.Entry<String, BaseType> field : fields.entrySet()) {
          String fieldName = field.getKey();
          BaseType fieldBaseType = field.getValue();
          System.err.println("name=" + fieldName + "; baseType=" + fieldBaseType);
          fieldOrdering.add(fieldName);
          FieldType fieldType = fieldBaseType.getType();
          System.err.println("fieldType=" + fieldType + "; token=" + fieldType.token);
          FieldType primitiveType = FieldType.byToken((byte) baseToken((int)fieldType.token & 0xff));
          String primTypeAsString = primitiveTypeAsString(primitiveType);
          String fieldTypeAsJsonString;
          if ((fieldType.token & 0xc0) == 0) {
            // classic primitive types
            fieldTypeAsJsonString = "[\"null\",\"" + primTypeAsString + "\"]";
          } else if (fieldType == FieldType.BYTE_ARRAY) {
            fieldTypeAsJsonString = "[\"null\",\"bytes\"]";
          } else if ((fieldType.token & 0x40) == 0) {
            // arrays of classic primitive types
            fieldTypeAsJsonString = "[\"null\",{\"type\":\"array\",\"items\":\""
                                  + primTypeAsString
                                  + "\"}\"";
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
            //    {{record, [index,int],[value,MyType]}}  //XXX
            fieldTypeAsJsonString = "[\"null\",{\"type\":\"array\","
                                  + "\"items\":{\"type\":\"record\":"
                                  +           "\"fields\":"
                                  +              "[{\"type\":\"int\",\"name\":\"index\"},"
                                  +              "{\"type\":\""+primTypeAsString+"\","
                                  +               " \"name\":\"value\"}]}"
                                  + "\"}]";
          }
          if (!isFirst) {
            pr.print(",\n");
          } else {
            pr.print("\n");
            isFirst = false;
          }
           
          pr.print("      { \"name\":\"" + fieldName
                    + ",\"type\":" + fieldTypeAsJsonString + "}");
        }
        pr.print("\n  ]\n}\n");
      } catch (FileNotFoundException ex) {
        throw new RuntimeException(ex);
      }
    }
    try (PrintWriter pr = new PrintWriter(outdir + "/" + "AnyEvent" + ".avsc")) {
      pr.print("{\n  \"type\":\"record\",\n"
               +  "  \"name\":\"" + "AnyEvent" + "\",\n"
               +  "  \"namespace\":\"com.openx\",\n"
               +  "  \"fields\":[\n"
               +  "    \"name\":\"event\",\n"
               +  "    \"type\":[");
      boolean isFirst = true;
      for (String name : eventNames) {
        pr.print((isFirst ? "\n" : ",\n")
                + "      \"" + name + "\"");
        isFirst = false;
      }
      pr.print("\n    ]\n  ]\n}\n");
    } catch (FileNotFoundException ex) {
      throw new RuntimeException(ex);
    }
  }
}
