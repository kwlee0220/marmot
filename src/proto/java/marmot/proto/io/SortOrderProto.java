// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.io.proto

package marmot.proto.io;

/**
 * Protobuf enum {@code marmot.proto.io.SortOrderProto}
 */
public enum SortOrderProto
    implements com.google.protobuf.ProtocolMessageEnum {
  /**
   * <code>ASC = 0;</code>
   */
  ASC(0),
  /**
   * <code>DESC = 1;</code>
   */
  DESC(1),
  UNRECOGNIZED(-1),
  ;

  /**
   * <code>ASC = 0;</code>
   */
  public static final int ASC_VALUE = 0;
  /**
   * <code>DESC = 1;</code>
   */
  public static final int DESC_VALUE = 1;


  public final int getNumber() {
    if (this == UNRECOGNIZED) {
      throw new java.lang.IllegalArgumentException(
          "Can't get the number of an unknown enum value.");
    }
    return value;
  }

  /**
   * @deprecated Use {@link #forNumber(int)} instead.
   */
  @java.lang.Deprecated
  public static SortOrderProto valueOf(int value) {
    return forNumber(value);
  }

  public static SortOrderProto forNumber(int value) {
    switch (value) {
      case 0: return ASC;
      case 1: return DESC;
      default: return null;
    }
  }

  public static com.google.protobuf.Internal.EnumLiteMap<SortOrderProto>
      internalGetValueMap() {
    return internalValueMap;
  }
  private static final com.google.protobuf.Internal.EnumLiteMap<
      SortOrderProto> internalValueMap =
        new com.google.protobuf.Internal.EnumLiteMap<SortOrderProto>() {
          public SortOrderProto findValueByNumber(int number) {
            return SortOrderProto.forNumber(number);
          }
        };

  public final com.google.protobuf.Descriptors.EnumValueDescriptor
      getValueDescriptor() {
    return getDescriptor().getValues().get(ordinal());
  }
  public final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptorForType() {
    return getDescriptor();
  }
  public static final com.google.protobuf.Descriptors.EnumDescriptor
      getDescriptor() {
    return marmot.proto.io.MarmotIo.getDescriptor().getEnumTypes().get(0);
  }

  private static final SortOrderProto[] VALUES = values();

  public static SortOrderProto valueOf(
      com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
    if (desc.getType() != getDescriptor()) {
      throw new java.lang.IllegalArgumentException(
        "EnumValueDescriptor is not for this type.");
    }
    if (desc.getIndex() == -1) {
      return UNRECOGNIZED;
    }
    return VALUES[desc.getIndex()];
  }

  private final int value;

  private SortOrderProto(int value) {
    this.value = value;
  }

  // @@protoc_insertion_point(enum_scope:marmot.proto.io.SortOrderProto)
}

