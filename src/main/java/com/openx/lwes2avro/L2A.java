package com.openx.lwes2avro;

import org.lwes.db.EventTemplateDB;

public class L2A {
  public static void main(String[] args) {
    String dbfile = args[0];
    String outdir = args[1];
    EventTemplateDB db = new EventTemplateDB();
    db.setESFFile(dbfile);
    db.initialize();
    for (Map.Entry<String, Map<String, BaseType>> entry : db.getEvents()) {
      String eventName = entry.getKey();
      Map<String, BaseType> fields = entry.getValue();
      PrintWriter pr = outdir + "/" + eventName + ".avsc";
      pr.print("{\n  \"type\":\"record\",\n  \"name\":\"" + eventName + "\",\n  \"namespace\":\"com.openx\",\n  \"fields\":[\n");
      for (Map.Entry<String, BaseType> field : fields) {
        String fieldName = field.getKey();
        BaseType fieldType = field.getValue();
        switch (fieldType.getType()) {
          case UINT16:
          case INT16:
          case UINT32:
          case INT32:
          case STRING:
          case IPADDR:
          case INT64:
          case UINT64:
          case BOOLEAN:
          case BYTE:
          case FLOAT:
          case DOUBLE:
          case UINT16_ARRAY:
          case INT16_ARRAY:
          case UINT32_ARRAY:
          case INT32_ARRAY:
          case STRING_ARRAY:
          case IP_ADDR_ARRAY:
          case INT64_ARRAY:
          case UINT64_ARRAY:
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
        }
      }

    public final byte                           token;
    public final String                         name;
    private Integer                             constantSize;
    private final boolean                       array, nullableArray;
    private FieldType                           componentType, arrayType, nullableArrayType;
    private final Object                        defaultValue;
    private static final FieldType[]            TYPES_BY_TOKEN = new FieldType[256];
    private static final Map<String, FieldType> TYPES_BY_NAME;

    private FieldType(int token, String name) {
        this(token, name, null);
    }

    private FieldType(int token, String name, Object defaultValue) {
        this.token = (byte) token;
        this.name = name;
        this.array = name.startsWith("[L");
        this.nullableArray = this.array && name().startsWith("N");
        this.defaultValue = defaultValue;
    }

    static {
        TYPES_BY_NAME        = new HashMap<String, FieldType>();
        for (FieldType type : values()) {
            TYPES_BY_TOKEN[type.token & 0xff] = type;
            TYPES_BY_NAME.put(type.name, type);
            
            if (type.isArray()) {
                // This will fail if our naming becomes inconsistent or a type starts with N.
                String name = type.name();
                name = name.replace("_ARRAY", "");
                name = name.replaceFirst("^N", "");
                name = name.replace("IP_ADDR", "IPADDR");  // due to formatting inconsistency
                final FieldType componentType = valueOf(name);
                type.componentType = componentType;
                if (type.isNullableArray()) {
                    componentType.nullableArrayType = type;
                } else {
                    componentType.arrayType = type;
                }
            }
        }
        BOOLEAN.constantSize = 1;
        BYTE.constantSize    = 1;
        INT16.constantSize   = 2;
        UINT16.constantSize  = 2;
        FLOAT.constantSize   = 4;
        INT32.constantSize   = 4;
        IPADDR.constantSize  = 4;
        UINT32.constantSize  = 4;
        INT64.constantSize   = 8;
        UINT64.constantSize  = 8;
        DOUBLE.constantSize  = 8;
    }

    public static FieldType byToken(byte token) {
        final FieldType type = TYPES_BY_TOKEN[token & 0xff];
        if (type == null) {
            throw new IllegalArgumentException("Bad token: " + token);
        }
        return type;
    }

    public static FieldType byName(String name) {
        final FieldType type = TYPES_BY_NAME.get(name);
        if (type == null) {
            throw new IllegalArgumentException("Bad field name: " + name);
        }
        return type;
    }

    @Override
    public String toString() {
        return name;
    }

    public boolean isNullableArray() {
        return nullableArray;
    }

    public boolean isArray() {
        return array;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public FieldType getNullableArrayType() {
        if (nullableArrayType == null) {
            if (isArray()) {
                throw new IllegalStateException(
                    "Multidimensional arrays are not supported; " + this + ".getArrayType() unsupported");
            } else {
                throw new IllegalStateException("Unsupported type: " + this);
            }
        }
        return nullableArrayType;
    }

    public FieldType getArrayType() {
        if (arrayType == null) {
            if (isArray()) {
                throw new IllegalStateException(
                    "Multidimensional arrays are not supported; " + this + ".getArrayType() unsupported");
            } else {
                throw new IllegalStateException("Unsupported type: " + this);
            }
        }
        return arrayType;
    }

    public FieldType getComponentType() {
      if (componentType == null) {
        throw new IllegalStateException(
            "Only array types provide component types " + this + ".getComponentType() unsupported");
      }
      return componentType;
    }

    public boolean isConstantSize() {
        return constantSize != null;
    }

    public int getConstantSize() {
        if (constantSize == null) {
            throw new IllegalStateException("Type "+this+" does not have a constant size");
        } else {
            return constantSize;
        }
    }

    public boolean isCompatibleWith(Object value) {
        if (value == null) {
            return true;
        }
        switch (this) {
            case BOOLEAN:
                return value instanceof Boolean;
            case BYTE:
                return value instanceof Byte;
            case DOUBLE:
                return value instanceof Double;
            case FLOAT:
                return value instanceof Float;
            case INT16:
                return value instanceof Short;
            case INT32:
                return value instanceof Integer;
            case INT64:
                return value instanceof Long;
            case IPADDR:
                return value instanceof IPAddress;
            case STRING:
                return value instanceof String;
            case UINT16:
                return value instanceof Integer;
            case UINT32:
                return value instanceof Long;
            case UINT64:
                return value instanceof BigInteger;
            case BOOLEAN_ARRAY:
                return value instanceof boolean[];
            case BYTE_ARRAY:
                return value instanceof byte[];
            case DOUBLE_ARRAY:
                return value instanceof double[];
            case FLOAT_ARRAY:
                return value instanceof float[];
            case INT16_ARRAY:
                return value instanceof short[];
            case INT32_ARRAY:
                return value instanceof int[];
            case INT64_ARRAY:
                return value instanceof long[];
            case IP_ADDR_ARRAY:
                return value instanceof IPAddress[];
            case STRING_ARRAY:
                return value instanceof String[];
            case UINT16_ARRAY:
                return value instanceof int[];
            case UINT32_ARRAY:
                return value instanceof long[];
            case UINT64_ARRAY:
                return value instanceof BigInteger[];
            case NBOOLEAN_ARRAY:
                return value instanceof Boolean[];
            case NBYTE_ARRAY:
                return value instanceof Byte[];
            case NDOUBLE_ARRAY:
                return value instanceof Double[];
            case NFLOAT_ARRAY:
                return value instanceof Float[];
            case NINT16_ARRAY:
                return value instanceof Short[];
            case NINT32_ARRAY:
                return value instanceof Integer[];
            case NINT64_ARRAY:
                return value instanceof Long[];
            case NSTRING_ARRAY:
                return value instanceof String[];
            case NUINT16_ARRAY:
                return value instanceof Integer[];
            case NUINT32_ARRAY:
                return value instanceof Long[];
            case NUINT64_ARRAY:
                return value instanceof BigInteger[];
        }
        throw new IllegalStateException("Unsupported type: " + this);
    }

    public void checkCompatibilityWith(Object typeObject) {
        if (! this.isCompatibleWith(typeObject)) {
            throw new NoSuchAttributeTypeException(String.format(
                "Wrong class '%s' for LWES type %s",
                typeObject.getClass().getName(), name()));
        }
    }
}
        }

        ...
      }
      pr.print("  ]\n}\n");

    }
  }
}
