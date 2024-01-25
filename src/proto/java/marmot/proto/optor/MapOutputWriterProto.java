// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.MapOutputWriterProto}
 */
public  final class MapOutputWriterProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.MapOutputWriterProto)
    MapOutputWriterProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use MapOutputWriterProto.newBuilder() to construct.
  private MapOutputWriterProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private MapOutputWriterProto() {
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private MapOutputWriterProto(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    if (extensionRegistry == null) {
      throw new java.lang.NullPointerException();
    }
    int mutable_bitField0_ = 0;
    com.google.protobuf.UnknownFieldSet.Builder unknownFields =
        com.google.protobuf.UnknownFieldSet.newBuilder();
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!parseUnknownFieldProto3(
                input, unknownFields, extensionRegistry, tag)) {
              done = true;
            }
            break;
          }
          case 10: {
            java.lang.String s = input.readStringRequireUtf8();
            optionalMapOutputKeyColsCase_ = 1;
            optionalMapOutputKeyCols_ = s;
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_MapOutputWriterProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_MapOutputWriterProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.MapOutputWriterProto.class, marmot.proto.optor.MapOutputWriterProto.Builder.class);
  }

  private int optionalMapOutputKeyColsCase_ = 0;
  private java.lang.Object optionalMapOutputKeyCols_;
  public enum OptionalMapOutputKeyColsCase
      implements com.google.protobuf.Internal.EnumLite {
    MAP_OUTPUT_KEY_COLS(1),
    OPTIONALMAPOUTPUTKEYCOLS_NOT_SET(0);
    private final int value;
    private OptionalMapOutputKeyColsCase(int value) {
      this.value = value;
    }
    /**
     * @deprecated Use {@link #forNumber(int)} instead.
     */
    @java.lang.Deprecated
    public static OptionalMapOutputKeyColsCase valueOf(int value) {
      return forNumber(value);
    }

    public static OptionalMapOutputKeyColsCase forNumber(int value) {
      switch (value) {
        case 1: return MAP_OUTPUT_KEY_COLS;
        case 0: return OPTIONALMAPOUTPUTKEYCOLS_NOT_SET;
        default: return null;
      }
    }
    public int getNumber() {
      return this.value;
    }
  };

  public OptionalMapOutputKeyColsCase
  getOptionalMapOutputKeyColsCase() {
    return OptionalMapOutputKeyColsCase.forNumber(
        optionalMapOutputKeyColsCase_);
  }

  public static final int MAP_OUTPUT_KEY_COLS_FIELD_NUMBER = 1;
  /**
   * <code>string map_output_key_cols = 1;</code>
   */
  public java.lang.String getMapOutputKeyCols() {
    java.lang.Object ref = "";
    if (optionalMapOutputKeyColsCase_ == 1) {
      ref = optionalMapOutputKeyCols_;
    }
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      if (optionalMapOutputKeyColsCase_ == 1) {
        optionalMapOutputKeyCols_ = s;
      }
      return s;
    }
  }
  /**
   * <code>string map_output_key_cols = 1;</code>
   */
  public com.google.protobuf.ByteString
      getMapOutputKeyColsBytes() {
    java.lang.Object ref = "";
    if (optionalMapOutputKeyColsCase_ == 1) {
      ref = optionalMapOutputKeyCols_;
    }
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      if (optionalMapOutputKeyColsCase_ == 1) {
        optionalMapOutputKeyCols_ = b;
      }
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (optionalMapOutputKeyColsCase_ == 1) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, optionalMapOutputKeyCols_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (optionalMapOutputKeyColsCase_ == 1) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, optionalMapOutputKeyCols_);
    }
    size += unknownFields.getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof marmot.proto.optor.MapOutputWriterProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.MapOutputWriterProto other = (marmot.proto.optor.MapOutputWriterProto) obj;

    boolean result = true;
    result = result && getOptionalMapOutputKeyColsCase().equals(
        other.getOptionalMapOutputKeyColsCase());
    if (!result) return false;
    switch (optionalMapOutputKeyColsCase_) {
      case 1:
        result = result && getMapOutputKeyCols()
            .equals(other.getMapOutputKeyCols());
        break;
      case 0:
      default:
    }
    result = result && unknownFields.equals(other.unknownFields);
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    switch (optionalMapOutputKeyColsCase_) {
      case 1:
        hash = (37 * hash) + MAP_OUTPUT_KEY_COLS_FIELD_NUMBER;
        hash = (53 * hash) + getMapOutputKeyCols().hashCode();
        break;
      case 0:
      default:
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.MapOutputWriterProto parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(marmot.proto.optor.MapOutputWriterProto prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code marmot.proto.optor.MapOutputWriterProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.MapOutputWriterProto)
      marmot.proto.optor.MapOutputWriterProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_MapOutputWriterProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_MapOutputWriterProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.MapOutputWriterProto.class, marmot.proto.optor.MapOutputWriterProto.Builder.class);
    }

    // Construct using marmot.proto.optor.MapOutputWriterProto.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      optionalMapOutputKeyColsCase_ = 0;
      optionalMapOutputKeyCols_ = null;
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_MapOutputWriterProto_descriptor;
    }

    public marmot.proto.optor.MapOutputWriterProto getDefaultInstanceForType() {
      return marmot.proto.optor.MapOutputWriterProto.getDefaultInstance();
    }

    public marmot.proto.optor.MapOutputWriterProto build() {
      marmot.proto.optor.MapOutputWriterProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.MapOutputWriterProto buildPartial() {
      marmot.proto.optor.MapOutputWriterProto result = new marmot.proto.optor.MapOutputWriterProto(this);
      if (optionalMapOutputKeyColsCase_ == 1) {
        result.optionalMapOutputKeyCols_ = optionalMapOutputKeyCols_;
      }
      result.optionalMapOutputKeyColsCase_ = optionalMapOutputKeyColsCase_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, java.lang.Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        java.lang.Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof marmot.proto.optor.MapOutputWriterProto) {
        return mergeFrom((marmot.proto.optor.MapOutputWriterProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.MapOutputWriterProto other) {
      if (other == marmot.proto.optor.MapOutputWriterProto.getDefaultInstance()) return this;
      switch (other.getOptionalMapOutputKeyColsCase()) {
        case MAP_OUTPUT_KEY_COLS: {
          optionalMapOutputKeyColsCase_ = 1;
          optionalMapOutputKeyCols_ = other.optionalMapOutputKeyCols_;
          onChanged();
          break;
        }
        case OPTIONALMAPOUTPUTKEYCOLS_NOT_SET: {
          break;
        }
      }
      this.mergeUnknownFields(other.unknownFields);
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      marmot.proto.optor.MapOutputWriterProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.MapOutputWriterProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int optionalMapOutputKeyColsCase_ = 0;
    private java.lang.Object optionalMapOutputKeyCols_;
    public OptionalMapOutputKeyColsCase
        getOptionalMapOutputKeyColsCase() {
      return OptionalMapOutputKeyColsCase.forNumber(
          optionalMapOutputKeyColsCase_);
    }

    public Builder clearOptionalMapOutputKeyCols() {
      optionalMapOutputKeyColsCase_ = 0;
      optionalMapOutputKeyCols_ = null;
      onChanged();
      return this;
    }


    /**
     * <code>string map_output_key_cols = 1;</code>
     */
    public java.lang.String getMapOutputKeyCols() {
      java.lang.Object ref = "";
      if (optionalMapOutputKeyColsCase_ == 1) {
        ref = optionalMapOutputKeyCols_;
      }
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (optionalMapOutputKeyColsCase_ == 1) {
          optionalMapOutputKeyCols_ = s;
        }
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string map_output_key_cols = 1;</code>
     */
    public com.google.protobuf.ByteString
        getMapOutputKeyColsBytes() {
      java.lang.Object ref = "";
      if (optionalMapOutputKeyColsCase_ == 1) {
        ref = optionalMapOutputKeyCols_;
      }
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        if (optionalMapOutputKeyColsCase_ == 1) {
          optionalMapOutputKeyCols_ = b;
        }
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string map_output_key_cols = 1;</code>
     */
    public Builder setMapOutputKeyCols(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  optionalMapOutputKeyColsCase_ = 1;
      optionalMapOutputKeyCols_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string map_output_key_cols = 1;</code>
     */
    public Builder clearMapOutputKeyCols() {
      if (optionalMapOutputKeyColsCase_ == 1) {
        optionalMapOutputKeyColsCase_ = 0;
        optionalMapOutputKeyCols_ = null;
        onChanged();
      }
      return this;
    }
    /**
     * <code>string map_output_key_cols = 1;</code>
     */
    public Builder setMapOutputKeyColsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      optionalMapOutputKeyColsCase_ = 1;
      optionalMapOutputKeyCols_ = value;
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFieldsProto3(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.MapOutputWriterProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.MapOutputWriterProto)
  private static final marmot.proto.optor.MapOutputWriterProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.MapOutputWriterProto();
  }

  public static marmot.proto.optor.MapOutputWriterProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<MapOutputWriterProto>
      PARSER = new com.google.protobuf.AbstractParser<MapOutputWriterProto>() {
    public MapOutputWriterProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new MapOutputWriterProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<MapOutputWriterProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<MapOutputWriterProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.MapOutputWriterProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

