package org.lwes.lwes2avro;


public class Helpers {
  public static int writeIntAvroZigzag(int v, int idx, byte[] out) {
    byte low;
    if (v < 0) {
      low = ((-(v+1))*2 + 1) & 0x7f;
      v = (-(v+1)) >> 6;
    } else {
      low = (v*2) & 0x7f;
      v = v >> 6;
    }
    if (v == 0) {
      out[idx++] = low;
      return idx;
    }

    out[idx++] = low | 0x80;
    for (;;) {
      byte next = v & 0x7f;
      v >>= 7;
      if (v == 0) {
        out[idx++] = next;
        return idx;
      } else {
        out[idx++] = next | 0x80;
      }
    }
  }
  public static int writeLongAvroZigzag(long v, int idx, byte[] out) {
    byte low;
    if (v < 0) {
      low = ((-(v+1))*2 + 1) & 0x7f;
      v = (-(v+1)) >> 6;
    } else {
      low = (v*2) & 0x7f;
      v = v >> 6;
    }
    if (v == 0) {
      out[idx++] = low;
      return idx;
    }

    out[idx++] = low | 0x80;
    for (;;) {
      byte next = v & 0x7f;
      v >>= 7;
      if (v == 0) {
        out[idx++] = next;
        return idx;
      } else {
        out[idx++] = next | 0x80;
      }
    }
  }
  public static int writeIPAddress(byte[] v, int idx, byte[] out) {
    out[idx++] = 8;
    out[idx++] = v[0];
    out[idx++] = v[1];
    out[idx++] = v[2];
    out[idx++] = v[3];
    return idx;
  }
  public static int writeFloat(float f, int idx, byte[] out) {
    int v = Float.floatToIntBits(f);
    out[idx++] = (byte) (v);
    out[idx++] = (byte) (v >> 8);
    out[idx++] = (byte) (v >> 16);
    out[idx++] = (byte) (v >> 24);
    return idx;
  }
  public static int writeDouble(double f, int idx, byte[] out) {
    long v = Double.doubleToLongBits(f);
    out[idx++] = (byte) (v);
    out[idx++] = (byte) (v >> 8);
    out[idx++] = (byte) (v >> 16);
    out[idx++] = (byte) (v >> 24);
    out[idx++] = (byte) (v >> 32);
    out[idx++] = (byte) (v >> 40);
    out[idx++] = (byte) (v >> 48);
    out[idx++] = (byte) (v >> 56);
    return idx;
  }
}
