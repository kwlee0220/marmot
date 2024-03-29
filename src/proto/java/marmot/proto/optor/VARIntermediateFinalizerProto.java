// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.VARIntermediateFinalizerProto}
 */
public  final class VARIntermediateFinalizerProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.VARIntermediateFinalizerProto)
    VARIntermediateFinalizerProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use VARIntermediateFinalizerProto.newBuilder() to construct.
  private VARIntermediateFinalizerProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private VARIntermediateFinalizerProto() {
    aggregators_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private VARIntermediateFinalizerProto(
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
            if (!((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
              aggregators_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000001;
            }
            aggregators_.add(s);
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
      if (((mutable_bitField0_ & 0x00000001) == 0x00000001)) {
        aggregators_ = aggregators_.getUnmodifiableView();
      }
      this.unknownFields = unknownFields.build();
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_VARIntermediateFinalizerProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_VARIntermediateFinalizerProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.VARIntermediateFinalizerProto.class, marmot.proto.optor.VARIntermediateFinalizerProto.Builder.class);
  }

  public static final int AGGREGATORS_FIELD_NUMBER = 1;
  private com.google.protobuf.LazyStringList aggregators_;
  /**
   * <code>repeated string aggregators = 1;</code>
   */
  public com.google.protobuf.ProtocolStringList
      getAggregatorsList() {
    return aggregators_;
  }
  /**
   * <code>repeated string aggregators = 1;</code>
   */
  public int getAggregatorsCount() {
    return aggregators_.size();
  }
  /**
   * <code>repeated string aggregators = 1;</code>
   */
  public java.lang.String getAggregators(int index) {
    return aggregators_.get(index);
  }
  /**
   * <code>repeated string aggregators = 1;</code>
   */
  public com.google.protobuf.ByteString
      getAggregatorsBytes(int index) {
    return aggregators_.getByteString(index);
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
    for (int i = 0; i < aggregators_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, aggregators_.getRaw(i));
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    {
      int dataSize = 0;
      for (int i = 0; i < aggregators_.size(); i++) {
        dataSize += computeStringSizeNoTag(aggregators_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getAggregatorsList().size();
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
    if (!(obj instanceof marmot.proto.optor.VARIntermediateFinalizerProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.VARIntermediateFinalizerProto other = (marmot.proto.optor.VARIntermediateFinalizerProto) obj;

    boolean result = true;
    result = result && getAggregatorsList()
        .equals(other.getAggregatorsList());
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
    if (getAggregatorsCount() > 0) {
      hash = (37 * hash) + AGGREGATORS_FIELD_NUMBER;
      hash = (53 * hash) + getAggregatorsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.VARIntermediateFinalizerProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.VARIntermediateFinalizerProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.VARIntermediateFinalizerProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.VARIntermediateFinalizerProto)
      marmot.proto.optor.VARIntermediateFinalizerProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_VARIntermediateFinalizerProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_VARIntermediateFinalizerProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.VARIntermediateFinalizerProto.class, marmot.proto.optor.VARIntermediateFinalizerProto.Builder.class);
    }

    // Construct using marmot.proto.optor.VARIntermediateFinalizerProto.newBuilder()
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
      aggregators_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_VARIntermediateFinalizerProto_descriptor;
    }

    public marmot.proto.optor.VARIntermediateFinalizerProto getDefaultInstanceForType() {
      return marmot.proto.optor.VARIntermediateFinalizerProto.getDefaultInstance();
    }

    public marmot.proto.optor.VARIntermediateFinalizerProto build() {
      marmot.proto.optor.VARIntermediateFinalizerProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.VARIntermediateFinalizerProto buildPartial() {
      marmot.proto.optor.VARIntermediateFinalizerProto result = new marmot.proto.optor.VARIntermediateFinalizerProto(this);
      int from_bitField0_ = bitField0_;
      if (((bitField0_ & 0x00000001) == 0x00000001)) {
        aggregators_ = aggregators_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000001);
      }
      result.aggregators_ = aggregators_;
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
      if (other instanceof marmot.proto.optor.VARIntermediateFinalizerProto) {
        return mergeFrom((marmot.proto.optor.VARIntermediateFinalizerProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.VARIntermediateFinalizerProto other) {
      if (other == marmot.proto.optor.VARIntermediateFinalizerProto.getDefaultInstance()) return this;
      if (!other.aggregators_.isEmpty()) {
        if (aggregators_.isEmpty()) {
          aggregators_ = other.aggregators_;
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          ensureAggregatorsIsMutable();
          aggregators_.addAll(other.aggregators_);
        }
        onChanged();
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
      marmot.proto.optor.VARIntermediateFinalizerProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.VARIntermediateFinalizerProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.LazyStringList aggregators_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensureAggregatorsIsMutable() {
      if (!((bitField0_ & 0x00000001) == 0x00000001)) {
        aggregators_ = new com.google.protobuf.LazyStringArrayList(aggregators_);
        bitField0_ |= 0x00000001;
       }
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public com.google.protobuf.ProtocolStringList
        getAggregatorsList() {
      return aggregators_.getUnmodifiableView();
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public int getAggregatorsCount() {
      return aggregators_.size();
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public java.lang.String getAggregators(int index) {
      return aggregators_.get(index);
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public com.google.protobuf.ByteString
        getAggregatorsBytes(int index) {
      return aggregators_.getByteString(index);
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public Builder setAggregators(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureAggregatorsIsMutable();
      aggregators_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public Builder addAggregators(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensureAggregatorsIsMutable();
      aggregators_.add(value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public Builder addAllAggregators(
        java.lang.Iterable<java.lang.String> values) {
      ensureAggregatorsIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, aggregators_);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public Builder clearAggregators() {
      aggregators_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string aggregators = 1;</code>
     */
    public Builder addAggregatorsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      ensureAggregatorsIsMutable();
      aggregators_.add(value);
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.VARIntermediateFinalizerProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.VARIntermediateFinalizerProto)
  private static final marmot.proto.optor.VARIntermediateFinalizerProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.VARIntermediateFinalizerProto();
  }

  public static marmot.proto.optor.VARIntermediateFinalizerProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<VARIntermediateFinalizerProto>
      PARSER = new com.google.protobuf.AbstractParser<VARIntermediateFinalizerProto>() {
    public VARIntermediateFinalizerProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new VARIntermediateFinalizerProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<VARIntermediateFinalizerProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<VARIntermediateFinalizerProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.VARIntermediateFinalizerProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

