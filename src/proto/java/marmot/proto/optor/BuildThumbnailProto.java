// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.BuildThumbnailProto}
 */
public  final class BuildThumbnailProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.BuildThumbnailProto)
    BuildThumbnailProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use BuildThumbnailProto.newBuilder() to construct.
  private BuildThumbnailProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private BuildThumbnailProto() {
    geomColumn_ = "";
    sampleRatio_ = 0D;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private BuildThumbnailProto(
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

            geomColumn_ = s;
            break;
          }
          case 17: {

            sampleRatio_ = input.readDouble();
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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_BuildThumbnailProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_BuildThumbnailProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.BuildThumbnailProto.class, marmot.proto.optor.BuildThumbnailProto.Builder.class);
  }

  public static final int GEOM_COLUMN_FIELD_NUMBER = 1;
  private volatile java.lang.Object geomColumn_;
  /**
   * <code>string geom_column = 1;</code>
   */
  public java.lang.String getGeomColumn() {
    java.lang.Object ref = geomColumn_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      geomColumn_ = s;
      return s;
    }
  }
  /**
   * <code>string geom_column = 1;</code>
   */
  public com.google.protobuf.ByteString
      getGeomColumnBytes() {
    java.lang.Object ref = geomColumn_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      geomColumn_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int SAMPLE_RATIO_FIELD_NUMBER = 2;
  private double sampleRatio_;
  /**
   * <code>double sample_ratio = 2;</code>
   */
  public double getSampleRatio() {
    return sampleRatio_;
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
    if (!getGeomColumnBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, geomColumn_);
    }
    if (sampleRatio_ != 0D) {
      output.writeDouble(2, sampleRatio_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getGeomColumnBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, geomColumn_);
    }
    if (sampleRatio_ != 0D) {
      size += com.google.protobuf.CodedOutputStream
        .computeDoubleSize(2, sampleRatio_);
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
    if (!(obj instanceof marmot.proto.optor.BuildThumbnailProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.BuildThumbnailProto other = (marmot.proto.optor.BuildThumbnailProto) obj;

    boolean result = true;
    result = result && getGeomColumn()
        .equals(other.getGeomColumn());
    result = result && (
        java.lang.Double.doubleToLongBits(getSampleRatio())
        == java.lang.Double.doubleToLongBits(
            other.getSampleRatio()));
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
    hash = (37 * hash) + GEOM_COLUMN_FIELD_NUMBER;
    hash = (53 * hash) + getGeomColumn().hashCode();
    hash = (37 * hash) + SAMPLE_RATIO_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        java.lang.Double.doubleToLongBits(getSampleRatio()));
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.BuildThumbnailProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.BuildThumbnailProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.BuildThumbnailProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.BuildThumbnailProto)
      marmot.proto.optor.BuildThumbnailProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_BuildThumbnailProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_BuildThumbnailProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.BuildThumbnailProto.class, marmot.proto.optor.BuildThumbnailProto.Builder.class);
    }

    // Construct using marmot.proto.optor.BuildThumbnailProto.newBuilder()
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
      geomColumn_ = "";

      sampleRatio_ = 0D;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_BuildThumbnailProto_descriptor;
    }

    public marmot.proto.optor.BuildThumbnailProto getDefaultInstanceForType() {
      return marmot.proto.optor.BuildThumbnailProto.getDefaultInstance();
    }

    public marmot.proto.optor.BuildThumbnailProto build() {
      marmot.proto.optor.BuildThumbnailProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.BuildThumbnailProto buildPartial() {
      marmot.proto.optor.BuildThumbnailProto result = new marmot.proto.optor.BuildThumbnailProto(this);
      result.geomColumn_ = geomColumn_;
      result.sampleRatio_ = sampleRatio_;
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
      if (other instanceof marmot.proto.optor.BuildThumbnailProto) {
        return mergeFrom((marmot.proto.optor.BuildThumbnailProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.BuildThumbnailProto other) {
      if (other == marmot.proto.optor.BuildThumbnailProto.getDefaultInstance()) return this;
      if (!other.getGeomColumn().isEmpty()) {
        geomColumn_ = other.geomColumn_;
        onChanged();
      }
      if (other.getSampleRatio() != 0D) {
        setSampleRatio(other.getSampleRatio());
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
      marmot.proto.optor.BuildThumbnailProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.BuildThumbnailProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object geomColumn_ = "";
    /**
     * <code>string geom_column = 1;</code>
     */
    public java.lang.String getGeomColumn() {
      java.lang.Object ref = geomColumn_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        geomColumn_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string geom_column = 1;</code>
     */
    public com.google.protobuf.ByteString
        getGeomColumnBytes() {
      java.lang.Object ref = geomColumn_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        geomColumn_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string geom_column = 1;</code>
     */
    public Builder setGeomColumn(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      geomColumn_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string geom_column = 1;</code>
     */
    public Builder clearGeomColumn() {
      
      geomColumn_ = getDefaultInstance().getGeomColumn();
      onChanged();
      return this;
    }
    /**
     * <code>string geom_column = 1;</code>
     */
    public Builder setGeomColumnBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      geomColumn_ = value;
      onChanged();
      return this;
    }

    private double sampleRatio_ ;
    /**
     * <code>double sample_ratio = 2;</code>
     */
    public double getSampleRatio() {
      return sampleRatio_;
    }
    /**
     * <code>double sample_ratio = 2;</code>
     */
    public Builder setSampleRatio(double value) {
      
      sampleRatio_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>double sample_ratio = 2;</code>
     */
    public Builder clearSampleRatio() {
      
      sampleRatio_ = 0D;
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.BuildThumbnailProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.BuildThumbnailProto)
  private static final marmot.proto.optor.BuildThumbnailProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.BuildThumbnailProto();
  }

  public static marmot.proto.optor.BuildThumbnailProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<BuildThumbnailProto>
      PARSER = new com.google.protobuf.AbstractParser<BuildThumbnailProto>() {
    public BuildThumbnailProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new BuildThumbnailProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<BuildThumbnailProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<BuildThumbnailProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.BuildThumbnailProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

