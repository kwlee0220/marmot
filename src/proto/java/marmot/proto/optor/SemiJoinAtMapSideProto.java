// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.SemiJoinAtMapSideProto}
 */
public  final class SemiJoinAtMapSideProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.SemiJoinAtMapSideProto)
    SemiJoinAtMapSideProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use SemiJoinAtMapSideProto.newBuilder() to construct.
  private SemiJoinAtMapSideProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private SemiJoinAtMapSideProto() {
    inputJoinColumns_ = "";
    paramDataset_ = "";
    paramJoinColumns_ = "";
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private SemiJoinAtMapSideProto(
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

            inputJoinColumns_ = s;
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            paramDataset_ = s;
            break;
          }
          case 26: {
            java.lang.String s = input.readStringRequireUtf8();

            paramJoinColumns_ = s;
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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SemiJoinAtMapSideProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SemiJoinAtMapSideProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.SemiJoinAtMapSideProto.class, marmot.proto.optor.SemiJoinAtMapSideProto.Builder.class);
  }

  public static final int INPUT_JOIN_COLUMNS_FIELD_NUMBER = 1;
  private volatile java.lang.Object inputJoinColumns_;
  /**
   * <code>string input_join_columns = 1;</code>
   */
  public java.lang.String getInputJoinColumns() {
    java.lang.Object ref = inputJoinColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      inputJoinColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string input_join_columns = 1;</code>
   */
  public com.google.protobuf.ByteString
      getInputJoinColumnsBytes() {
    java.lang.Object ref = inputJoinColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      inputJoinColumns_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PARAM_DATASET_FIELD_NUMBER = 2;
  private volatile java.lang.Object paramDataset_;
  /**
   * <code>string param_dataset = 2;</code>
   */
  public java.lang.String getParamDataset() {
    java.lang.Object ref = paramDataset_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      paramDataset_ = s;
      return s;
    }
  }
  /**
   * <code>string param_dataset = 2;</code>
   */
  public com.google.protobuf.ByteString
      getParamDatasetBytes() {
    java.lang.Object ref = paramDataset_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      paramDataset_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int PARAM_JOIN_COLUMNS_FIELD_NUMBER = 3;
  private volatile java.lang.Object paramJoinColumns_;
  /**
   * <code>string param_join_columns = 3;</code>
   */
  public java.lang.String getParamJoinColumns() {
    java.lang.Object ref = paramJoinColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      paramJoinColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string param_join_columns = 3;</code>
   */
  public com.google.protobuf.ByteString
      getParamJoinColumnsBytes() {
    java.lang.Object ref = paramJoinColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      paramJoinColumns_ = b;
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
    if (!getInputJoinColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, inputJoinColumns_);
    }
    if (!getParamDatasetBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, paramDataset_);
    }
    if (!getParamJoinColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, paramJoinColumns_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getInputJoinColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, inputJoinColumns_);
    }
    if (!getParamDatasetBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, paramDataset_);
    }
    if (!getParamJoinColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, paramJoinColumns_);
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
    if (!(obj instanceof marmot.proto.optor.SemiJoinAtMapSideProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.SemiJoinAtMapSideProto other = (marmot.proto.optor.SemiJoinAtMapSideProto) obj;

    boolean result = true;
    result = result && getInputJoinColumns()
        .equals(other.getInputJoinColumns());
    result = result && getParamDataset()
        .equals(other.getParamDataset());
    result = result && getParamJoinColumns()
        .equals(other.getParamJoinColumns());
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
    hash = (37 * hash) + INPUT_JOIN_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getInputJoinColumns().hashCode();
    hash = (37 * hash) + PARAM_DATASET_FIELD_NUMBER;
    hash = (53 * hash) + getParamDataset().hashCode();
    hash = (37 * hash) + PARAM_JOIN_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getParamJoinColumns().hashCode();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.SemiJoinAtMapSideProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.SemiJoinAtMapSideProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.SemiJoinAtMapSideProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.SemiJoinAtMapSideProto)
      marmot.proto.optor.SemiJoinAtMapSideProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SemiJoinAtMapSideProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SemiJoinAtMapSideProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.SemiJoinAtMapSideProto.class, marmot.proto.optor.SemiJoinAtMapSideProto.Builder.class);
    }

    // Construct using marmot.proto.optor.SemiJoinAtMapSideProto.newBuilder()
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
      inputJoinColumns_ = "";

      paramDataset_ = "";

      paramJoinColumns_ = "";

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_SemiJoinAtMapSideProto_descriptor;
    }

    public marmot.proto.optor.SemiJoinAtMapSideProto getDefaultInstanceForType() {
      return marmot.proto.optor.SemiJoinAtMapSideProto.getDefaultInstance();
    }

    public marmot.proto.optor.SemiJoinAtMapSideProto build() {
      marmot.proto.optor.SemiJoinAtMapSideProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.SemiJoinAtMapSideProto buildPartial() {
      marmot.proto.optor.SemiJoinAtMapSideProto result = new marmot.proto.optor.SemiJoinAtMapSideProto(this);
      result.inputJoinColumns_ = inputJoinColumns_;
      result.paramDataset_ = paramDataset_;
      result.paramJoinColumns_ = paramJoinColumns_;
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
      if (other instanceof marmot.proto.optor.SemiJoinAtMapSideProto) {
        return mergeFrom((marmot.proto.optor.SemiJoinAtMapSideProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.SemiJoinAtMapSideProto other) {
      if (other == marmot.proto.optor.SemiJoinAtMapSideProto.getDefaultInstance()) return this;
      if (!other.getInputJoinColumns().isEmpty()) {
        inputJoinColumns_ = other.inputJoinColumns_;
        onChanged();
      }
      if (!other.getParamDataset().isEmpty()) {
        paramDataset_ = other.paramDataset_;
        onChanged();
      }
      if (!other.getParamJoinColumns().isEmpty()) {
        paramJoinColumns_ = other.paramJoinColumns_;
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
      marmot.proto.optor.SemiJoinAtMapSideProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.SemiJoinAtMapSideProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object inputJoinColumns_ = "";
    /**
     * <code>string input_join_columns = 1;</code>
     */
    public java.lang.String getInputJoinColumns() {
      java.lang.Object ref = inputJoinColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        inputJoinColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string input_join_columns = 1;</code>
     */
    public com.google.protobuf.ByteString
        getInputJoinColumnsBytes() {
      java.lang.Object ref = inputJoinColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        inputJoinColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string input_join_columns = 1;</code>
     */
    public Builder setInputJoinColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      inputJoinColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string input_join_columns = 1;</code>
     */
    public Builder clearInputJoinColumns() {
      
      inputJoinColumns_ = getDefaultInstance().getInputJoinColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string input_join_columns = 1;</code>
     */
    public Builder setInputJoinColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      inputJoinColumns_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object paramDataset_ = "";
    /**
     * <code>string param_dataset = 2;</code>
     */
    public java.lang.String getParamDataset() {
      java.lang.Object ref = paramDataset_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        paramDataset_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string param_dataset = 2;</code>
     */
    public com.google.protobuf.ByteString
        getParamDatasetBytes() {
      java.lang.Object ref = paramDataset_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        paramDataset_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string param_dataset = 2;</code>
     */
    public Builder setParamDataset(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      paramDataset_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string param_dataset = 2;</code>
     */
    public Builder clearParamDataset() {
      
      paramDataset_ = getDefaultInstance().getParamDataset();
      onChanged();
      return this;
    }
    /**
     * <code>string param_dataset = 2;</code>
     */
    public Builder setParamDatasetBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      paramDataset_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object paramJoinColumns_ = "";
    /**
     * <code>string param_join_columns = 3;</code>
     */
    public java.lang.String getParamJoinColumns() {
      java.lang.Object ref = paramJoinColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        paramJoinColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string param_join_columns = 3;</code>
     */
    public com.google.protobuf.ByteString
        getParamJoinColumnsBytes() {
      java.lang.Object ref = paramJoinColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        paramJoinColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string param_join_columns = 3;</code>
     */
    public Builder setParamJoinColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      paramJoinColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string param_join_columns = 3;</code>
     */
    public Builder clearParamJoinColumns() {
      
      paramJoinColumns_ = getDefaultInstance().getParamJoinColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string param_join_columns = 3;</code>
     */
    public Builder setParamJoinColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      paramJoinColumns_ = value;
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.SemiJoinAtMapSideProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.SemiJoinAtMapSideProto)
  private static final marmot.proto.optor.SemiJoinAtMapSideProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.SemiJoinAtMapSideProto();
  }

  public static marmot.proto.optor.SemiJoinAtMapSideProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<SemiJoinAtMapSideProto>
      PARSER = new com.google.protobuf.AbstractParser<SemiJoinAtMapSideProto>() {
    public SemiJoinAtMapSideProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new SemiJoinAtMapSideProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<SemiJoinAtMapSideProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<SemiJoinAtMapSideProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.SemiJoinAtMapSideProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

