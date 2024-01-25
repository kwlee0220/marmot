// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.TakeNByGroupAtMapSideProto}
 */
public  final class TakeNByGroupAtMapSideProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.TakeNByGroupAtMapSideProto)
    TakeNByGroupAtMapSideProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use TakeNByGroupAtMapSideProto.newBuilder() to construct.
  private TakeNByGroupAtMapSideProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private TakeNByGroupAtMapSideProto() {
    keyColumns_ = "";
    tagColumns_ = "";
    orderColumns_ = "";
    takeCount_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private TakeNByGroupAtMapSideProto(
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

            keyColumns_ = s;
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            tagColumns_ = s;
            break;
          }
          case 26: {
            java.lang.String s = input.readStringRequireUtf8();

            orderColumns_ = s;
            break;
          }
          case 32: {

            takeCount_ = input.readInt32();
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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_TakeNByGroupAtMapSideProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_TakeNByGroupAtMapSideProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.TakeNByGroupAtMapSideProto.class, marmot.proto.optor.TakeNByGroupAtMapSideProto.Builder.class);
  }

  public static final int KEY_COLUMNS_FIELD_NUMBER = 1;
  private volatile java.lang.Object keyColumns_;
  /**
   * <code>string key_columns = 1;</code>
   */
  public java.lang.String getKeyColumns() {
    java.lang.Object ref = keyColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      keyColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string key_columns = 1;</code>
   */
  public com.google.protobuf.ByteString
      getKeyColumnsBytes() {
    java.lang.Object ref = keyColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      keyColumns_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TAG_COLUMNS_FIELD_NUMBER = 2;
  private volatile java.lang.Object tagColumns_;
  /**
   * <code>string tag_columns = 2;</code>
   */
  public java.lang.String getTagColumns() {
    java.lang.Object ref = tagColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      tagColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string tag_columns = 2;</code>
   */
  public com.google.protobuf.ByteString
      getTagColumnsBytes() {
    java.lang.Object ref = tagColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      tagColumns_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ORDER_COLUMNS_FIELD_NUMBER = 3;
  private volatile java.lang.Object orderColumns_;
  /**
   * <code>string order_columns = 3;</code>
   */
  public java.lang.String getOrderColumns() {
    java.lang.Object ref = orderColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      orderColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string order_columns = 3;</code>
   */
  public com.google.protobuf.ByteString
      getOrderColumnsBytes() {
    java.lang.Object ref = orderColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      orderColumns_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int TAKE_COUNT_FIELD_NUMBER = 4;
  private int takeCount_;
  /**
   * <code>int32 take_count = 4;</code>
   */
  public int getTakeCount() {
    return takeCount_;
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
    if (!getKeyColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, keyColumns_);
    }
    if (!getTagColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, tagColumns_);
    }
    if (!getOrderColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, orderColumns_);
    }
    if (takeCount_ != 0) {
      output.writeInt32(4, takeCount_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getKeyColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, keyColumns_);
    }
    if (!getTagColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, tagColumns_);
    }
    if (!getOrderColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, orderColumns_);
    }
    if (takeCount_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(4, takeCount_);
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
    if (!(obj instanceof marmot.proto.optor.TakeNByGroupAtMapSideProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.TakeNByGroupAtMapSideProto other = (marmot.proto.optor.TakeNByGroupAtMapSideProto) obj;

    boolean result = true;
    result = result && getKeyColumns()
        .equals(other.getKeyColumns());
    result = result && getTagColumns()
        .equals(other.getTagColumns());
    result = result && getOrderColumns()
        .equals(other.getOrderColumns());
    result = result && (getTakeCount()
        == other.getTakeCount());
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
    hash = (37 * hash) + KEY_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getKeyColumns().hashCode();
    hash = (37 * hash) + TAG_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getTagColumns().hashCode();
    hash = (37 * hash) + ORDER_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getOrderColumns().hashCode();
    hash = (37 * hash) + TAKE_COUNT_FIELD_NUMBER;
    hash = (53 * hash) + getTakeCount();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.TakeNByGroupAtMapSideProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.TakeNByGroupAtMapSideProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.TakeNByGroupAtMapSideProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.TakeNByGroupAtMapSideProto)
      marmot.proto.optor.TakeNByGroupAtMapSideProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_TakeNByGroupAtMapSideProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_TakeNByGroupAtMapSideProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.TakeNByGroupAtMapSideProto.class, marmot.proto.optor.TakeNByGroupAtMapSideProto.Builder.class);
    }

    // Construct using marmot.proto.optor.TakeNByGroupAtMapSideProto.newBuilder()
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
      keyColumns_ = "";

      tagColumns_ = "";

      orderColumns_ = "";

      takeCount_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_TakeNByGroupAtMapSideProto_descriptor;
    }

    public marmot.proto.optor.TakeNByGroupAtMapSideProto getDefaultInstanceForType() {
      return marmot.proto.optor.TakeNByGroupAtMapSideProto.getDefaultInstance();
    }

    public marmot.proto.optor.TakeNByGroupAtMapSideProto build() {
      marmot.proto.optor.TakeNByGroupAtMapSideProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.TakeNByGroupAtMapSideProto buildPartial() {
      marmot.proto.optor.TakeNByGroupAtMapSideProto result = new marmot.proto.optor.TakeNByGroupAtMapSideProto(this);
      result.keyColumns_ = keyColumns_;
      result.tagColumns_ = tagColumns_;
      result.orderColumns_ = orderColumns_;
      result.takeCount_ = takeCount_;
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
      if (other instanceof marmot.proto.optor.TakeNByGroupAtMapSideProto) {
        return mergeFrom((marmot.proto.optor.TakeNByGroupAtMapSideProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.TakeNByGroupAtMapSideProto other) {
      if (other == marmot.proto.optor.TakeNByGroupAtMapSideProto.getDefaultInstance()) return this;
      if (!other.getKeyColumns().isEmpty()) {
        keyColumns_ = other.keyColumns_;
        onChanged();
      }
      if (!other.getTagColumns().isEmpty()) {
        tagColumns_ = other.tagColumns_;
        onChanged();
      }
      if (!other.getOrderColumns().isEmpty()) {
        orderColumns_ = other.orderColumns_;
        onChanged();
      }
      if (other.getTakeCount() != 0) {
        setTakeCount(other.getTakeCount());
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
      marmot.proto.optor.TakeNByGroupAtMapSideProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.TakeNByGroupAtMapSideProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object keyColumns_ = "";
    /**
     * <code>string key_columns = 1;</code>
     */
    public java.lang.String getKeyColumns() {
      java.lang.Object ref = keyColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        keyColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string key_columns = 1;</code>
     */
    public com.google.protobuf.ByteString
        getKeyColumnsBytes() {
      java.lang.Object ref = keyColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        keyColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string key_columns = 1;</code>
     */
    public Builder setKeyColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      keyColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string key_columns = 1;</code>
     */
    public Builder clearKeyColumns() {
      
      keyColumns_ = getDefaultInstance().getKeyColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string key_columns = 1;</code>
     */
    public Builder setKeyColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      keyColumns_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object tagColumns_ = "";
    /**
     * <code>string tag_columns = 2;</code>
     */
    public java.lang.String getTagColumns() {
      java.lang.Object ref = tagColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        tagColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string tag_columns = 2;</code>
     */
    public com.google.protobuf.ByteString
        getTagColumnsBytes() {
      java.lang.Object ref = tagColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        tagColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string tag_columns = 2;</code>
     */
    public Builder setTagColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      tagColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string tag_columns = 2;</code>
     */
    public Builder clearTagColumns() {
      
      tagColumns_ = getDefaultInstance().getTagColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string tag_columns = 2;</code>
     */
    public Builder setTagColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      tagColumns_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object orderColumns_ = "";
    /**
     * <code>string order_columns = 3;</code>
     */
    public java.lang.String getOrderColumns() {
      java.lang.Object ref = orderColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        orderColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string order_columns = 3;</code>
     */
    public com.google.protobuf.ByteString
        getOrderColumnsBytes() {
      java.lang.Object ref = orderColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        orderColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string order_columns = 3;</code>
     */
    public Builder setOrderColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      orderColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string order_columns = 3;</code>
     */
    public Builder clearOrderColumns() {
      
      orderColumns_ = getDefaultInstance().getOrderColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string order_columns = 3;</code>
     */
    public Builder setOrderColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      orderColumns_ = value;
      onChanged();
      return this;
    }

    private int takeCount_ ;
    /**
     * <code>int32 take_count = 4;</code>
     */
    public int getTakeCount() {
      return takeCount_;
    }
    /**
     * <code>int32 take_count = 4;</code>
     */
    public Builder setTakeCount(int value) {
      
      takeCount_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int32 take_count = 4;</code>
     */
    public Builder clearTakeCount() {
      
      takeCount_ = 0;
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.TakeNByGroupAtMapSideProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.TakeNByGroupAtMapSideProto)
  private static final marmot.proto.optor.TakeNByGroupAtMapSideProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.TakeNByGroupAtMapSideProto();
  }

  public static marmot.proto.optor.TakeNByGroupAtMapSideProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<TakeNByGroupAtMapSideProto>
      PARSER = new com.google.protobuf.AbstractParser<TakeNByGroupAtMapSideProto>() {
    public TakeNByGroupAtMapSideProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new TakeNByGroupAtMapSideProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<TakeNByGroupAtMapSideProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<TakeNByGroupAtMapSideProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.TakeNByGroupAtMapSideProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}
