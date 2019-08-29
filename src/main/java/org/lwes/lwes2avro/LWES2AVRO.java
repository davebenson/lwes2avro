package org.lwes.lwes2avro;

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

    for (Map.Entry<String, Map<String, BaseType>> entry : db.getEvents()) {
      String eventName = entry.getKey();
      eventNames.add(eventName);
      Map<String, BaseType> fields = entry.getValue();
      ArrayList<String> fieldOrdering = new ArrayList<>();
      try (PrintWriter pr = outdir + "/" + eventName + ".avsc") {
        pr.print("{\n  \"type\":\"record\",\n"
                    "  \"name\":\"" + eventName + "\",\n"
                    "  \"namespace\":\"com.openx\",\n"
                    "  \"fields\":[\n");
        fieldsByEvent.put(eventName, fieldOrdering);
        for (Map.Entry<String, BaseType> field : fields) {
          String fieldName = field.getKey();
          BaseType fieldBaseType = field.getValue();
          fieldOrdering.add(fieldName);
          FieldType fieldType = fieldBaseType.getType();
          FieldType primitiveType = FieldType.byToken(fieldType.token & 0x3f);
          String primTypeAsString = primitiveTypeAsString(primitiveType);
          if ((fieldType.token & 0xc0) == 0) {
            // classic primitive types
            fieldTypeAsJsonString = "[null,\"" + primTypeAsString + "\"]";
          } else if ((fieldType.token & 0x40) == 0) {
            // arrays of classic primitive types
            fieldTypeAsJsonString = "[null,{\"type\":\"array\",\"items\":\""
                                  + primTypeAsString
                                  + "\"}\";
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
            fieldTypeAsJsonString = "[null,{\"type\":\"array\","
                                  + "\"items\":{\"type\":\"record\":"
                                  +           "\"fields\":"
                                  +              "[{\"type\":\"int\",\"name\":\"index\"},"
                                  +              "{\"type\":\""+primTypeAsString+"\","
                                  +               " \"name\":\"value\"}]}"
                                  + "\"}]";
          }
          if (!isFirst) {
            pr.print(",\n");
            isFirst = false;
          }
          pr.print("  { \"name\":\"" + fieldName
                    + ",\"type\":" + fieldTypeAsJsonString + "}");
        }
        pr.print("  ]\n}\n");
      }

      //
      // Part 2. Emit org.lwes.Event -> avro binary-data code.
      //
      try (PrintWriter pr = outdir + "/" + eventName + ".java") {
        pr.print("package ...;\n");
        pr.print("import org.lwes.Event;\n");
        pr.print("import org.lwes.lwes2avro.Helpers;\n");
        pr.print("public class " + event + " {\n"
               + "  static int pack(Event event, byte[] out) {\n");
        pr.print("  int outIndex = 0;\n");
        for (String fieldName : fieldOrdering) {
          BaseType baseType = fields.get(fieldName);
          FieldType fieldType = baseType.getType();
          FieldType token = fieldType.token;
          FieldType primitiveType = FieldType.byToken(fieldType.token & 0x3f);
          String primTypeAsString = primitiveTypeAsString(primitiveType);
          pr.print("  if (event.hasField(\"" + fieldName + "\") {\n");
          pr.print("    out[outIndex++] = 2;\n");
          switch (fieldType) {
            case UINT16:
              pr.print("    Integer v = event.getUInt16(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v, outIndex, out);\n");
              break;
            case INT16:
              pr.print("    Short v = event.getInt16(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v, outIndex, out);\n");
              break;
            case UINT32:
              pr.print("    Long v = event.getUInt32(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeLongAvroZigzag(v, outIndex, out);\n");
              break;
            case INT32:
              pr.print("    Integer v = event.getInt32(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v, outIndex, out);\n");
              break;
            case STRING:
              pr.print("    String v = event.getString(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeString(v, outIndex, out);\n");
              break;
            case IPADDR:
              pr.print("    byte[] v = event.getIPAddress(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeIPAddress(v, outIndex, out);\n");
              break;
            case INT64:
              pr.print("    byte[] v = event.getIPAddress(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeLongAvroZigzag(v, outIndex, out);\n");
              break;
            case UINT64:
              pr.print("    BigInteger v = event.getUInt64(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeBigIntegerAvroZigzag(v, outIndex, out);\n");
              break;
            case BOOLEAN:
              pr.print("    boolean v = event.getBoolean(\"" + fieldName \");\n");
              pr.print("    outIndex = Helpers.writeBoolean(v, outIndex, out);\n");
              break;
            case BYTE:
              pr.print("    int v = event.getByte(\"" + fieldName \");\n");
              pr.print("    v &= 0xff;\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v, outIndex, out);\n");
              break;
            case FLOAT:
              pr.print("    outIndex = Helpers.writeFloat(event.getFloat(\"" + fieldName \"));\n");
              break;
            case DOUBLE:
              pr.print("    outIndex = Helpers.writeDouble(event.getDouble(\"" + fieldName \"));\n");
              break;
            case UINT16_ARRAY:
              pr.print("    int[] v = event.getUInt16Array(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeIntAvroZigzag(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case INT16_ARRAY:
              pr.print("    short[] v = event.getInt16Array(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeIntAvroZigzag(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case UINT32_ARRAY:
              pr.print("    long[] v = event.getUInt32Array(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeLongAvroZigzag(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case INT32_ARRAY:
              pr.print("    int[] v = event.getInt32Array(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeIntAvroZigzag(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case STRING_ARRAY:
              pr.print("    String[] v = event.getStringArray(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeString(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case IP_ADDR_ARRAY:
              throw new RuntimeException("lwes-java doesn't support arrays of ip-addresses");
            case INT64_ARRAY:
              pr.print("    long[] v = event.getInt64Array(\"" + fieldName + "\");\n");
              pr.print("    outIndex = Helpers.writeIntAvroZigzag(v.length, outIndex, out);\n");
              pr.print("    for (int i = 0; i < v.length; i++)\n");
              pr.print("      outIndex = Helpers.writeLongAvroZigzag(v[i], outIndex, out);\n");
              pr.print("    out[outIndex++] = 0;\n");
              break;
            case UINT64_ARRAY:
              ...
            case BOOLEAN_ARRAY:
            case BYTE_ARRAY:
            case FLOAT_ARRAY:
            case DOUBLE_ARRAY:
            case NUINT16_ARRAY:
            case NINT16_ARRAY:
            case NUINT32_ARRAY:
            case NINT32_ARRAY:
            case NSTRING_ARRAY:
            case NINT64_ARRAY:
            case NUINT64_ARRAY:
            case NBOOLEAN_ARRAY:
            case NBYTE_ARRAY:
            case NFLOAT_ARRAY:
            case NDOUBLE_ARRAY:
:
          }
          } else 

      }
    }
  }
}
