
public class AvroSerializer {
  public AvroSerializer(EventTemplateDB db) {
    this.db = db;
    this.eventNameToIndex = new HashMap<String, Integer>();
    this.eventNameToFieldList = new HashMap<String, String[]>();
    Map<String, Map<String, BaseType>> lwesEvents = db.getEvents();
    this.eventNames = new String[lwesEvents.size()];
    int eventIndex = 0;

    for (Map.Entry<String, Map<String, BaseType>> entry : db.getEvents()) {
      String eventName = entry.getKey();
      eventNames[eventIndex] = eventName;
      eventNames.add(eventName);
      eventNameToIndex.put(eventName, eventIndex);
      Map<String, BaseType> fields = entry.getValue();
      String[] fieldOrdering = new String[fields.size()];
      StringBuffer sb = new StringBuffer();
      sb.append("  {\n  \"type\":\"record\",\n"
                "    \"name\":\"" + eventName + "\",\n"
                "    \"namespace\":\"com.openx\",\n"
                "    \"fields\":[\n");
      eventNameToFieldList.put(eventName, fieldOrdering);
      for (Map.Entry<String, BaseType> field : fields) {
        String fieldName = field.getKey();
        ...
      }
    }
  }

  //
  // Serialize the event, as the union type that contains all events in the esf file.
  //
  public int serialize(Event event, int startIndex, byte[] data) {
  }

  public class WriterConfig {
    WriterConfig() {
      deflate = true;
      deflateLevel = 6;

      // This value 'maxEventBytesPerBlock' is made up by me (daveb)
      // based on the notion that the default
      // zlib window size is 1<<16.
      //
      // So, it may require 1<<16 bytes
      // of data to be operating at max compression.
      //
      // (Usually, it gets very close to optimal very fast.
      // So these calculations are probably conservative, and it's possible
      // that maxEventBytesPerBlock could be divided by 10 w/o much size impact 
      // but we'll use 10% of the memory.)
      //
      // So, we use a buffer-size == 1<<19 that should get us
      // w/i 12.5% == 1/8 == 2**(-3) == 2**(16-19)
      // of compression-reduction (== 1.0 - compression_ratio)
      //
      // (if we used an infinite buffer).
      //
      // and in practice will get us much closer.
      maxEventBytesPerBlock = 1 << 19;
    }
    public boolean deflate;
    public int deflateLevel;
    public int maxEventsPerBlock;
    public int maxEventBytesPerBlock;
    public int initialDataBlockSize;
  }
public static WriterConfig defaultWriterConfig = new WriterConfig();

  /**
   * @param os An output stream to write the data to.
   *           <p>
   *           We always write large chunks of data at a time.
   *           so we recommend NOT adding additional buffering.
   *           <p>
   *           We also deflate by default in the file format,
   *           so recommend against using compression on this output stream.
   *
   *           <p>
   *           Also, each 'write' call (after the header, the first call)
   *           is independent, and so can be reordered
   *           without breaking the file format.
   *           (of course, this will scramble the records, but this is
   *           ok for raw events)
   */
  public Writer createWriter(OutputStream os, WriterConfig cfg) {
    return new Writer(os, cfg);
  }
  public Writer createWriter(OutputStream os) {
    return createWriter(os, defaultWriterConfig);
  }

  public class Writer {
    // Use createWriter() methods instead.
    private Writer(OutputStream os, WriterConfig config) {
      this.os = os;
      this.deflate = config.deflate;
      this.deflateLevel = config.deflateLevel;
      this.syncMarker = genRandom16();
      this.curRecords = 0;
      this.curSize = 0;
      this.rawDataBlock = new byte[config.initialDataBlockSize];

      // Write header.
      ...
    }
    public void write(Event event) throws UnknownEventTypeException,
                                          BadEventException {
      if (this.nRawEvents >= maxEventsPerBlock) {
        flushBuffer();
      }
      for (;;) {
        try {                                          
          rawDataBlockIndex = serialize(event, rawDataBlockIndex, rawDataBlock);
          nRawEvents++;
          return;
        } catch (ArrayOutOfBoundsException ex) {
          if (rawDataBlock.length < maxEventBytesPerBlock) {
            ... resize and retry
          } else if (rawDataBlockIndex == 0) {
            ... event too long!
          } else {
            // flush buffer, and then retry with empty buffer.
            flushBuffer();
          }
        }
      }
    }

    private void flushBuffer() {
      ... deflate or not

      ... write header and data
    }

    public void close() {
      if (nRawEvents > 0) {
        flushBuffer();
      }
      os.close();
    }

    private OutputStream os;
    private boolean deflate;
    private int deflateLevel;
    private byte[] syncMarker;
  }

  private final EventTemplateDB db;
  private final HashMap<String, Integer> eventNameToIndex;
  private final HashMap<String, String[]> eventNameToFieldList;
  private final String[] eventNames;
  private final byte[] avscDataMetadataProperty;
  private byte[] rawDataBlock;                  // pre-codec

  private static final byte[] avcodecNoneMetadataProperty = ...;
  private static final byte[] avcodecDeflateMetadataProperty = ...;

  private void 

}
