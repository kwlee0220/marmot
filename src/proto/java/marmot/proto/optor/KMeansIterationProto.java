// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

/**
 * Protobuf type {@code marmot.proto.optor.KMeansIterationProto}
 */
public  final class KMeansIterationProto extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:marmot.proto.optor.KMeansIterationProto)
    KMeansIterationProtoOrBuilder {
private static final long serialVersionUID = 0L;
  // Use KMeansIterationProto.newBuilder() to construct.
  private KMeansIterationProto(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private KMeansIterationProto() {
    workspacePath_ = "";
    featureColumns_ = "";
    clusterColumn_ = "";
    centroids_ = com.google.protobuf.ByteString.EMPTY;
    workerCount_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return this.unknownFields;
  }
  private KMeansIterationProto(
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

            workspacePath_ = s;
            break;
          }
          case 18: {
            java.lang.String s = input.readStringRequireUtf8();

            featureColumns_ = s;
            break;
          }
          case 26: {
            java.lang.String s = input.readStringRequireUtf8();

            clusterColumn_ = s;
            break;
          }
          case 34: {

            centroids_ = input.readBytes();
            break;
          }
          case 40: {

            workerCount_ = input.readInt32();
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
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_KMeansIterationProto_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_KMeansIterationProto_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            marmot.proto.optor.KMeansIterationProto.class, marmot.proto.optor.KMeansIterationProto.Builder.class);
  }

  public static final int WORKSPACE_PATH_FIELD_NUMBER = 1;
  private volatile java.lang.Object workspacePath_;
  /**
   * <code>string workspace_path = 1;</code>
   */
  public java.lang.String getWorkspacePath() {
    java.lang.Object ref = workspacePath_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      workspacePath_ = s;
      return s;
    }
  }
  /**
   * <code>string workspace_path = 1;</code>
   */
  public com.google.protobuf.ByteString
      getWorkspacePathBytes() {
    java.lang.Object ref = workspacePath_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      workspacePath_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int FEATURE_COLUMNS_FIELD_NUMBER = 2;
  private volatile java.lang.Object featureColumns_;
  /**
   * <code>string feature_columns = 2;</code>
   */
  public java.lang.String getFeatureColumns() {
    java.lang.Object ref = featureColumns_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      featureColumns_ = s;
      return s;
    }
  }
  /**
   * <code>string feature_columns = 2;</code>
   */
  public com.google.protobuf.ByteString
      getFeatureColumnsBytes() {
    java.lang.Object ref = featureColumns_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      featureColumns_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int CLUSTER_COLUMN_FIELD_NUMBER = 3;
  private volatile java.lang.Object clusterColumn_;
  /**
   * <code>string cluster_column = 3;</code>
   */
  public java.lang.String getClusterColumn() {
    java.lang.Object ref = clusterColumn_;
    if (ref instanceof java.lang.String) {
      return (java.lang.String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      java.lang.String s = bs.toStringUtf8();
      clusterColumn_ = s;
      return s;
    }
  }
  /**
   * <code>string cluster_column = 3;</code>
   */
  public com.google.protobuf.ByteString
      getClusterColumnBytes() {
    java.lang.Object ref = clusterColumn_;
    if (ref instanceof java.lang.String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (java.lang.String) ref);
      clusterColumn_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int CENTROIDS_FIELD_NUMBER = 4;
  private com.google.protobuf.ByteString centroids_;
  /**
   * <code>bytes centroids = 4;</code>
   */
  public com.google.protobuf.ByteString getCentroids() {
    return centroids_;
  }

  public static final int WORKER_COUNT_FIELD_NUMBER = 5;
  private int workerCount_;
  /**
   * <code>int32 worker_count = 5;</code>
   */
  public int getWorkerCount() {
    return workerCount_;
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
    if (!getWorkspacePathBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, workspacePath_);
    }
    if (!getFeatureColumnsBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, featureColumns_);
    }
    if (!getClusterColumnBytes().isEmpty()) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, clusterColumn_);
    }
    if (!centroids_.isEmpty()) {
      output.writeBytes(4, centroids_);
    }
    if (workerCount_ != 0) {
      output.writeInt32(5, workerCount_);
    }
    unknownFields.writeTo(output);
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!getWorkspacePathBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, workspacePath_);
    }
    if (!getFeatureColumnsBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, featureColumns_);
    }
    if (!getClusterColumnBytes().isEmpty()) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, clusterColumn_);
    }
    if (!centroids_.isEmpty()) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(4, centroids_);
    }
    if (workerCount_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(5, workerCount_);
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
    if (!(obj instanceof marmot.proto.optor.KMeansIterationProto)) {
      return super.equals(obj);
    }
    marmot.proto.optor.KMeansIterationProto other = (marmot.proto.optor.KMeansIterationProto) obj;

    boolean result = true;
    result = result && getWorkspacePath()
        .equals(other.getWorkspacePath());
    result = result && getFeatureColumns()
        .equals(other.getFeatureColumns());
    result = result && getClusterColumn()
        .equals(other.getClusterColumn());
    result = result && getCentroids()
        .equals(other.getCentroids());
    result = result && (getWorkerCount()
        == other.getWorkerCount());
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
    hash = (37 * hash) + WORKSPACE_PATH_FIELD_NUMBER;
    hash = (53 * hash) + getWorkspacePath().hashCode();
    hash = (37 * hash) + FEATURE_COLUMNS_FIELD_NUMBER;
    hash = (53 * hash) + getFeatureColumns().hashCode();
    hash = (37 * hash) + CLUSTER_COLUMN_FIELD_NUMBER;
    hash = (53 * hash) + getClusterColumn().hashCode();
    hash = (37 * hash) + CENTROIDS_FIELD_NUMBER;
    hash = (53 * hash) + getCentroids().hashCode();
    hash = (37 * hash) + WORKER_COUNT_FIELD_NUMBER;
    hash = (53 * hash) + getWorkerCount();
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.KMeansIterationProto parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.KMeansIterationProto parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static marmot.proto.optor.KMeansIterationProto parseFrom(
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
  public static Builder newBuilder(marmot.proto.optor.KMeansIterationProto prototype) {
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
   * Protobuf type {@code marmot.proto.optor.KMeansIterationProto}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:marmot.proto.optor.KMeansIterationProto)
      marmot.proto.optor.KMeansIterationProtoOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_KMeansIterationProto_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_KMeansIterationProto_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              marmot.proto.optor.KMeansIterationProto.class, marmot.proto.optor.KMeansIterationProto.Builder.class);
    }

    // Construct using marmot.proto.optor.KMeansIterationProto.newBuilder()
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
      workspacePath_ = "";

      featureColumns_ = "";

      clusterColumn_ = "";

      centroids_ = com.google.protobuf.ByteString.EMPTY;

      workerCount_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return marmot.proto.optor.MarmotOptorInternal.internal_static_marmot_proto_optor_KMeansIterationProto_descriptor;
    }

    public marmot.proto.optor.KMeansIterationProto getDefaultInstanceForType() {
      return marmot.proto.optor.KMeansIterationProto.getDefaultInstance();
    }

    public marmot.proto.optor.KMeansIterationProto build() {
      marmot.proto.optor.KMeansIterationProto result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public marmot.proto.optor.KMeansIterationProto buildPartial() {
      marmot.proto.optor.KMeansIterationProto result = new marmot.proto.optor.KMeansIterationProto(this);
      result.workspacePath_ = workspacePath_;
      result.featureColumns_ = featureColumns_;
      result.clusterColumn_ = clusterColumn_;
      result.centroids_ = centroids_;
      result.workerCount_ = workerCount_;
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
      if (other instanceof marmot.proto.optor.KMeansIterationProto) {
        return mergeFrom((marmot.proto.optor.KMeansIterationProto)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(marmot.proto.optor.KMeansIterationProto other) {
      if (other == marmot.proto.optor.KMeansIterationProto.getDefaultInstance()) return this;
      if (!other.getWorkspacePath().isEmpty()) {
        workspacePath_ = other.workspacePath_;
        onChanged();
      }
      if (!other.getFeatureColumns().isEmpty()) {
        featureColumns_ = other.featureColumns_;
        onChanged();
      }
      if (!other.getClusterColumn().isEmpty()) {
        clusterColumn_ = other.clusterColumn_;
        onChanged();
      }
      if (other.getCentroids() != com.google.protobuf.ByteString.EMPTY) {
        setCentroids(other.getCentroids());
      }
      if (other.getWorkerCount() != 0) {
        setWorkerCount(other.getWorkerCount());
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
      marmot.proto.optor.KMeansIterationProto parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (marmot.proto.optor.KMeansIterationProto) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private java.lang.Object workspacePath_ = "";
    /**
     * <code>string workspace_path = 1;</code>
     */
    public java.lang.String getWorkspacePath() {
      java.lang.Object ref = workspacePath_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        workspacePath_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string workspace_path = 1;</code>
     */
    public com.google.protobuf.ByteString
        getWorkspacePathBytes() {
      java.lang.Object ref = workspacePath_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        workspacePath_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string workspace_path = 1;</code>
     */
    public Builder setWorkspacePath(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      workspacePath_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string workspace_path = 1;</code>
     */
    public Builder clearWorkspacePath() {
      
      workspacePath_ = getDefaultInstance().getWorkspacePath();
      onChanged();
      return this;
    }
    /**
     * <code>string workspace_path = 1;</code>
     */
    public Builder setWorkspacePathBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      workspacePath_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object featureColumns_ = "";
    /**
     * <code>string feature_columns = 2;</code>
     */
    public java.lang.String getFeatureColumns() {
      java.lang.Object ref = featureColumns_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        featureColumns_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string feature_columns = 2;</code>
     */
    public com.google.protobuf.ByteString
        getFeatureColumnsBytes() {
      java.lang.Object ref = featureColumns_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        featureColumns_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string feature_columns = 2;</code>
     */
    public Builder setFeatureColumns(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      featureColumns_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string feature_columns = 2;</code>
     */
    public Builder clearFeatureColumns() {
      
      featureColumns_ = getDefaultInstance().getFeatureColumns();
      onChanged();
      return this;
    }
    /**
     * <code>string feature_columns = 2;</code>
     */
    public Builder setFeatureColumnsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      featureColumns_ = value;
      onChanged();
      return this;
    }

    private java.lang.Object clusterColumn_ = "";
    /**
     * <code>string cluster_column = 3;</code>
     */
    public java.lang.String getClusterColumn() {
      java.lang.Object ref = clusterColumn_;
      if (!(ref instanceof java.lang.String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        clusterColumn_ = s;
        return s;
      } else {
        return (java.lang.String) ref;
      }
    }
    /**
     * <code>string cluster_column = 3;</code>
     */
    public com.google.protobuf.ByteString
        getClusterColumnBytes() {
      java.lang.Object ref = clusterColumn_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        clusterColumn_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string cluster_column = 3;</code>
     */
    public Builder setClusterColumn(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      clusterColumn_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>string cluster_column = 3;</code>
     */
    public Builder clearClusterColumn() {
      
      clusterColumn_ = getDefaultInstance().getClusterColumn();
      onChanged();
      return this;
    }
    /**
     * <code>string cluster_column = 3;</code>
     */
    public Builder setClusterColumnBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      
      clusterColumn_ = value;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString centroids_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>bytes centroids = 4;</code>
     */
    public com.google.protobuf.ByteString getCentroids() {
      return centroids_;
    }
    /**
     * <code>bytes centroids = 4;</code>
     */
    public Builder setCentroids(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      centroids_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>bytes centroids = 4;</code>
     */
    public Builder clearCentroids() {
      
      centroids_ = getDefaultInstance().getCentroids();
      onChanged();
      return this;
    }

    private int workerCount_ ;
    /**
     * <code>int32 worker_count = 5;</code>
     */
    public int getWorkerCount() {
      return workerCount_;
    }
    /**
     * <code>int32 worker_count = 5;</code>
     */
    public Builder setWorkerCount(int value) {
      
      workerCount_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>int32 worker_count = 5;</code>
     */
    public Builder clearWorkerCount() {
      
      workerCount_ = 0;
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


    // @@protoc_insertion_point(builder_scope:marmot.proto.optor.KMeansIterationProto)
  }

  // @@protoc_insertion_point(class_scope:marmot.proto.optor.KMeansIterationProto)
  private static final marmot.proto.optor.KMeansIterationProto DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new marmot.proto.optor.KMeansIterationProto();
  }

  public static marmot.proto.optor.KMeansIterationProto getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<KMeansIterationProto>
      PARSER = new com.google.protobuf.AbstractParser<KMeansIterationProto>() {
    public KMeansIterationProto parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return new KMeansIterationProto(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<KMeansIterationProto> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<KMeansIterationProto> getParserForType() {
    return PARSER;
  }

  public marmot.proto.optor.KMeansIterationProto getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

