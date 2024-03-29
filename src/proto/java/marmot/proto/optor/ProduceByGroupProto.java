// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.ProduceByGroupProto}
 */
public  final class ProduceByGroupProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.ProduceByGroupProto)
    ProduceByGroupProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use ProduceByGroupProto.newBuilder() to construct.
  private ProduceByGroupProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private ProduceByGroupProto() {
    keyColumns_ = "";
    tagColumns_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private ProduceByGroupProto(
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
            marmot.proto.SerializedProto.Builder subBuilder = null;
            if (producer_ != null) {
              subBuilder = producer_.toBuilder();
            }
            producer_ = input.readMessage(marmot.proto.SerializedProto.parser(), extensionRegistry);
            if (subBuilder != null) {
              subBuilder.mergeFrom(producer_);
              producer_ = subBuilder.buildPartial();
            }

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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_ProduceByGroupProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_ProduceByGroupProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.ProduceByGroupProto.class, marmot.proto.optor.ProduceByGroupProto.Builder.class);
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

  public static final int PRODUCER_FIELD_NUMBER = 3;
  private marmot.proto.SerializedProto producer_;
  /**
   * <code>.marmot.proto.SerializedProto producer = 3;</code>
   */
  public boolean hasProducer() {
    return producer_ != null;
  }
  /**
   * <code>.marmot.proto.SerializedProto producer = 3;</code>
   */
  public marmot.proto.SerializedProto getProducer() {
    return producer_ == null ? marmot.proto.SerializedProto.getDefaultInstance() : producer_;
  }
  /**
   * <code>.marmot.proto.SerializedProto producer = 3;</code>
   */
  public marmot.proto.SerializedProtoOrBuilder getProducerOrBuilder() {
    return getProducer();
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
    if (producer_ != null) {
      output.writeMessage(3, getProducer());
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
    if (producer_ != null) {
      size += com.google.protobuf.CodedOutputStream
        .computeMessageSize(3, getProducer());
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
    if (!(obj instanceof marmot.proto.optor.ProduceByGroupProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.ProduceByGroupProto other = (marmot.proto.optor.ProduceByGroupProto) obj;

    boolean result = true;
    result = result && getKeyColumns()
        .equals(other.getKeyColumns());
    result = result && getTagColumns()
        .equals(other.getTagColumns());
    result = result && (hasProducer() == other.hasProducer());
    if (hasProducer()) {
      result = result && getProducer()
          .equals(other.getProducer());
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
    hash = (37 * hash) + KEY_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getKeyColumns().hashCode();
    hash = (37 * hash) + TAG_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getTagColumns().hashCode();
    if (hasProducer()) {
      hash = (37 * hash) + PRODUCER_FIELD_NUMBER;
      hash = (53 * hash) + getProducer().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.ProduceByGroupProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.ProduceByGroupProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.ProduceByGroupProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.ProduceByGroupProto)
      marmot.proto.optor.ProduceByGroupProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_ProduceByGroupProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_ProduceByGroupProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.ProduceByGroupProto.class, marmot.proto.optor.ProduceByGroupProto.Builder.class);
    }

    // Construct using marmot.proto.optor.ProduceByGroupProto.newBuilder()
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

      if (producerBuilder_ == null) {
        producer_ = null;
      } else {
        producer_ = null;
        producerBuilder_ = null;
      }
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_ProduceByGroupProto_descriptor;
    }

    public marmot.proto.optor.ProduceByGroupProto getDefaultInstanceForType() {
      return marmot.proto.optor.ProduceByGroupProto.getDefaultInstance();
    }

    public marmot.proto.optor.ProduceByGroupProto build() {
      marmot.proto.optor.ProduceByGroupProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.ProduceByGroupProto buildPartial() {
      marmot.proto.optor.ProduceByGroupProto result = new marmot.proto.optor.ProduceByGroupProto(this);
      result.keyColumns_ = keyColumns_;
      result.tagColumns_ = tagColumns_;
      if (producerBuilder_ == null) {
        result.producer_ = producer_;
      } else {
        result.producer_ = producerBuilder_.build();
      }
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
      if (other instanceof marmot.proto.optor.ProduceByGroupProto) {
        return mergeFrom((marmot.proto.optor.ProduceByGroupProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.ProduceByGroupProto other) {
      if (other == marmot.proto.optor.ProduceByGroupProto.getDefaultInstance()) return this;
      if (!other.getKeyColumns().isEmpty()) {
        keyColumns_ = other.keyColumns_;
        onChanged();
      }
      if (!other.getTagColumns().isEmpty()) {
        tagColumns_ = other.tagColumns_;
        onChanged();
      }
      if (other.hasProducer()) {
        mergeProducer(other.getProducer());
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
      marmot.proto.optor.ProduceByGroupProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.ProduceByGroupProto) e.getUnfinishedMessage();
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

    private marmot.proto.SerializedProto producer_ = null;
    private com.google.protobuf.SingleFieldBuilderV3<
        marmot.proto.SerializedProto, marmot.proto.SerializedProto.Builder, marmot.proto.SerializedProtoOrBuilder> producerBuilder_;
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public boolean hasProducer() {
      return producerBuilder_ != null || producer_ != null;
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public marmot.proto.SerializedProto getProducer() {
      if (producerBuilder_ == null) {
        return producer_ == null ? marmot.proto.SerializedProto.getDefaultInstance() : producer_;
      } else {
        return producerBuilder_.getMessage();
      }
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public Builder setProducer(marmot.proto.SerializedProto value) {
      if (producerBuilder_ == null) {
        if (value == null) {
          throw new NullPointerException();
        }
        producer_ = value;
        onChanged();
      } else {
        producerBuilder_.setMessage(value);
      }

      return this;
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public Builder setProducer(
        marmot.proto.SerializedProto.Builder builderForValue) {
      if (producerBuilder_ == null) {
        producer_ = builderForValue.build();
        onChanged();
      } else {
        producerBuilder_.setMessage(builderForValue.build());
      }

      return this;
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public Builder mergeProducer(marmot.proto.SerializedProto value) {
      if (producerBuilder_ == null) {
        if (producer_ != null) {
          producer_ =
            marmot.proto.SerializedProto.newBuilder(producer_).mergeFrom(value).buildPartial();
        } else {
          producer_ = value;
        }
        onChanged();
      } else {
        producerBuilder_.mergeFrom(value);
      }

      return this;
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public Builder clearProducer() {
      if (producerBuilder_ == null) {
        producer_ = null;
        onChanged();
      } else {
        producer_ = null;
        producerBuilder_ = null;
      }

      return this;
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public marmot.proto.SerializedProto.Builder getProducerBuilder() {
      
      onChanged();
      return getProducerFieldBuilder().getBuilder();
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    public marmot.proto.SerializedProtoOrBuilder getProducerOrBuilder() {
      if (producerBuilder_ != null) {
        return producerBuilder_.getMessageOrBuilder();
      } else {
        return producer_ == null ?
            marmot.proto.SerializedProto.getDefaultInstance() : producer_;
      }
    }
    /**
     * <code>.marmot.proto.SerializedProto producer = 3;</code>
     */
    private com.google.protobuf.SingleFieldBuilderV3<
        marmot.proto.SerializedProto, marmot.proto.SerializedProto.Builder, marmot.proto.SerializedProtoOrBuilder> 
        getProducerFieldBuilder() {
      if (producerBuilder_ == null) {
        producerBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
            marmot.proto.SerializedProto, marmot.proto.SerializedProto.Builder, marmot.proto.SerializedProtoOrBuilder>(
                getProducer(),
                getParentForChildren(),
                isClean());
        producer_ = null;
      }
      return producerBuilder_;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFieldsProto3(unknownFields);
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.ProduceByGroupProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.ProduceByGroupProto)
  private static final marmot.proto.optor.ProduceByGroupProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.ProduceByGroupProto();
  }

  public static marmot.proto.optor.ProduceByGroupProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<ProduceByGroupProto>
      PARSER = new com.google.protobuf.AbstractParser<ProduceByGroupProto>() {
    public ProduceByGroupProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new ProduceByGroupProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<ProduceByGroupProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<ProduceByGroupProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.ProduceByGroupProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

