// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.SplitQuadSpaceProto}
 */
public  final class SplitQuadSpaceProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.SplitQuadSpaceProto)
    SplitQuadSpaceProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SplitQuadSpaceProto.newBuilder() to construct.
  private SplitQuadSpaceProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SplitQuadSpaceProto() {
    maxQuadkeyLength_ = 0;
    splitSize_ = 0L;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SplitQuadSpaceProto(
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
            marmot.proto.GeometryColumnInfoProto.Builder subBuilder = null;
            if (geometryColumnInfo_ != null) {
              subBuilder = geometryColumnInfo_.toBuilder();
            }
            geometryColumnInfo_ = input.readMessage(marmot.proto.GeometryColumnInfoProto.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(geometryColumnInfo_);
              geometryColumnInfo_ = subBuilder.buildPartial();
            }

            break;
          }
          case 16: {

            maxQuadkeyLength_ = input.readInt32();
            break;
          }
          case 24: {

            splitSize_ = input.readInt64();
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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SplitQuadSpaceProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SplitQuadSpaceProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.SplitQuadSpaceProto.class, marmot.proto.optor.SplitQuadSpaceProto.Builder.class);
  }

  public static final int GEOMETRY_COLUMN_INFO_FIELD_NUMBER = 1;
  private marmot.proto.GeometryColumnInfoProto geometryColumnInfo_;
  /**
   * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
   */
  public boolean hasGeometryColumnInfo() {
    return geometryColumnInfo_ != null;
  }
  /**
   * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
   */
  public marmot.proto.GeometryColumnInfoProto getGeometryColumnInfo() {
    return geometryColumnInfo_ == null ? marmot.proto.GeometryColumnInfoProto.getDefaultInstance() : geometryColumnInfo_;
  }
  /**
   * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
   */
  public marmot.proto.GeometryColumnInfoProtoOrBuilder getGeometryColumnInfoOrBuilder() {
    return getGeometryColumnInfo();
  }

  public static final int MAX_QUADKEY_LENGTH_FIELD_NUMBER = 2;
  private int maxQuadkeyLength_;
  /**
   * <code>int32 max_quadkey_length = 2;</code>
   */
  public int getMaxQuadkeyLength() {
    return maxQuadkeyLength_;
  }

  public static final int SPLIT_SIZE_FIELD_NUMBER = 3;
  private long splitSize_;
  /**
   * <code>int64 split_size = 3;</code>
   */
  public long getSplitSize() {
    return splitSize_;
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
    if (geometryColumnInfo_ != null) {
      output.writeMessage(1, getGeometryColumnInfo());
    }
    if (maxQuadkeyLength_ != 0) {
      output.writeInt32(2, maxQuadkeyLength_);
    }
    if (splitSize_ != 0L) {
      output.writeInt64(3, splitSize_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (geometryColumnInfo_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(1, getGeometryColumnInfo());
    }
    if (maxQuadkeyLength_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(2, maxQuadkeyLength_);
    }
    if (splitSize_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(3, splitSize_);
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
    if (!(obj instanceof marmot.proto.optor.SplitQuadSpaceProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.SplitQuadSpaceProto other = (marmot.proto.optor.SplitQuadSpaceProto) obj;

    boolean result = true;
    result = result && (hasGeometryColumnInfo() == other.hasGeometryColumnInfo());
    if (hasGeometryColumnInfo()) {
      result = result && getGeometryColumnInfo()
          .equals(other.getGeometryColumnInfo());
    }
    result = result && (getMaxQuadkeyLength()
        == other.getMaxQuadkeyLength());
    result = result && (getSplitSize()
        == other.getSplitSize());
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
    if (hasGeometryColumnInfo()) {
      hash = (37 * hash) + GEOMETRY_COLUMN_INFO_FIELD_NUMBER;
      hash = (53 * hash) + getGeometryColumnInfo().hashCode();
    }
    hash = (37 * hash) + MAX_QUADKEY_LENGTH_FIELD_NUMBER;
    hash = (53 * hash) + getMaxQuadkeyLength();
    hash = (37 * hash) + SPLIT_SIZE_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getSplitSize());
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SplitQuadSpaceProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.SplitQuadSpaceProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.SplitQuadSpaceProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.SplitQuadSpaceProto)
      marmot.proto.optor.SplitQuadSpaceProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SplitQuadSpaceProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SplitQuadSpaceProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.SplitQuadSpaceProto.class, marmot.proto.optor.SplitQuadSpaceProto.Builder.class);
    }

    // Construct using marmot.proto.optor.SplitQuadSpaceProto.newBuilder()
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
      if (geometryColumnInfoBuilder_ == null) {
        geometryColumnInfo_ = null;
      } else {
        geometryColumnInfo_ = null;
        geometryColumnInfoBuilder_ = null;
      }
      maxQuadkeyLength_ = 0;

      splitSize_ = 0L;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SplitQuadSpaceProto_descriptor;
    }

    public marmot.proto.optor.SplitQuadSpaceProto getDefaultInstanceForType() {
      return marmot.proto.optor.SplitQuadSpaceProto.getDefaultInstance();
    }

    public marmot.proto.optor.SplitQuadSpaceProto build() {
      marmot.proto.optor.SplitQuadSpaceProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.SplitQuadSpaceProto buildPartial() {
      marmot.proto.optor.SplitQuadSpaceProto result = new marmot.proto.optor.SplitQuadSpaceProto(this);
      if (geometryColumnInfoBuilder_ == null) {
        result.geometryColumnInfo_ = geometryColumnInfo_;
      } else {
        result.geometryColumnInfo_ = geometryColumnInfoBuilder_.build();
      }
      result.maxQuadkeyLength_ = maxQuadkeyLength_;
      result.splitSize_ = splitSize_;
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
      if (other instanceof marmot.proto.optor.SplitQuadSpaceProto) {
        return mergeFrom((marmot.proto.optor.SplitQuadSpaceProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.SplitQuadSpaceProto other) {
      if (other == marmot.proto.optor.SplitQuadSpaceProto.getDefaultInstance()) return this;
      if (other.hasGeometryColumnInfo()) {
        mergeGeometryColumnInfo(other.getGeometryColumnInfo());
      }
      if (other.getMaxQuadkeyLength() != 0) {
        setMaxQuadkeyLength(other.getMaxQuadkeyLength());
      }
      if (other.getSplitSize() != 0L) {
        setSplitSize(other.getSplitSize());
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
      marmot.proto.optor.SplitQuadSpaceProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.SplitQuadSpaceProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private marmot.proto.GeometryColumnInfoProto geometryColumnInfo_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        marmot.proto.GeometryColumnInfoProto, marmot.proto.GeometryColumnInfoProto.Builder, marmot.proto.GeometryColumnInfoProtoOrBuilder> geometryColumnInfoBuilder_;
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public boolean hasGeometryColumnInfo() {
      return geometryColumnInfoBuilder_ != null || geometryColumnInfo_ != null;
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public marmot.proto.GeometryColumnInfoProto getGeometryColumnInfo() {
      if (geometryColumnInfoBuilder_ == null) {
        return geometryColumnInfo_ == null ? marmot.proto.GeometryColumnInfoProto.getDefaultInstance() : geometryColumnInfo_;
      } else {
        return geometryColumnInfoBuilder_.getMessage();
      }
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public Builder setGeometryColumnInfo(marmot.proto.GeometryColumnInfoProto value) {
      if (geometryColumnInfoBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        geometryColumnInfo_ = value;
        onChanged();
      } else {
        geometryColumnInfoBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public Builder setGeometryColumnInfo(
        marmot.proto.GeometryColumnInfoProto.Builder builderForValue) {
      if (geometryColumnInfoBuilder_ == null) {
        geometryColumnInfo_ = builderForValue.build();
        onChanged();
      } else {
        geometryColumnInfoBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public Builder mergeGeometryColumnInfo(marmot.proto.GeometryColumnInfoProto value) {
      if (geometryColumnInfoBuilder_ == null) {
        if (geometryColumnInfo_ != null) {
          geometryColumnInfo_ =
            marmot.proto.GeometryColumnInfoProto.newBuilder(geometryColumnInfo_).mergeFrom(value).buildPartial();
        } else {
          geometryColumnInfo_ = value;
        }
        onChanged();
      } else {
        geometryColumnInfoBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public Builder clearGeometryColumnInfo() {
      if (geometryColumnInfoBuilder_ == null) {
        geometryColumnInfo_ = null;
        onChanged();
      } else {
        geometryColumnInfo_ = null;
        geometryColumnInfoBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public marmot.proto.GeometryColumnInfoProto.Builder getGeometryColumnInfoBuilder() {
      
      onChanged();
      return getGeometryColumnInfoFieldBuilder().getBuilder();
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    public marmot.proto.GeometryColumnInfoProtoOrBuilder getGeometryColumnInfoOrBuilder() {
      if (geometryColumnInfoBuilder_ != null) {
        return geometryColumnInfoBuilder_.getMessageOrBuilder();
      } else {
        return geometryColumnInfo_ == null ?
            marmot.proto.GeometryColumnInfoProto.getDefaultInstance() : geometryColumnInfo_;
      }
    }
    /**
     * <code>.marmot.proto.GeometryColumnInfoProto geometry_column_info = 1;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        marmot.proto.GeometryColumnInfoProto, marmot.proto.GeometryColumnInfoProto.Builder, marmot.proto.GeometryColumnInfoProtoOrBuilder> 
        getGeometryColumnInfoFieldBuilder() {
      if (geometryColumnInfoBuilder_ == null) {
        geometryColumnInfoBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            marmot.proto.GeometryColumnInfoProto, marmot.proto.GeometryColumnInfoProto.Builder, marmot.proto.GeometryColumnInfoProtoOrBuilder>(
                getGeometryColumnInfo(),
                getParentForChildren(),
                isClean());
        geometryColumnInfo_ = null;
      }
      return geometryColumnInfoBuilder_;
    }

    private int maxQuadkeyLength_ ;
    /**
     * <code>int32 max_quadkey_length = 2;</code>
     */
    public int getMaxQuadkeyLength() {
      return maxQuadkeyLength_;
    }
    /**
     * <code>int32 max_quadkey_length = 2;</code>
     */
    public Builder setMaxQuadkeyLength(int value) {
      
      maxQuadkeyLength_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int32 max_quadkey_length = 2;</code>
     */
    public Builder clearMaxQuadkeyLength() {
      
      maxQuadkeyLength_ = 0;
      onChanged();
      return this;
    }

    private long splitSize_ ;
    /**
     * <code>int64 split_size = 3;</code>
     */
    public long getSplitSize() {
      return splitSize_;
    }
    /**
     * <code>int64 split_size = 3;</code>
     */
    public Builder setSplitSize(long value) {
      
      splitSize_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int64 split_size = 3;</code>
     */
    public Builder clearSplitSize() {
      
      splitSize_ = 0L;
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.SplitQuadSpaceProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.SplitQuadSpaceProto)
  private static final marmot.proto.optor.SplitQuadSpaceProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.SplitQuadSpaceProto();
  }

  public static marmot.proto.optor.SplitQuadSpaceProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<SplitQuadSpaceProto>
      PARSER = new com.google.protobuf.AbstractParser<SplitQuadSpaceProto>() {
    public SplitQuadSpaceProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SplitQuadSpaceProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SplitQuadSpaceProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<SplitQuadSpaceProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.SplitQuadSpaceProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
