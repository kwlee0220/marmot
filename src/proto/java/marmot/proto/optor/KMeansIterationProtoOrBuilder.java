// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

public interface KMeansIterationProtoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:marmot.proto.optor.KMeansIterationProto)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string workspace_path = 1;</code>
   */
  java.lang.String getWorkspacePath();
  /**
   * <code>string workspace_path = 1;</code>
   */
  com.google.protobuf.ByteString
      getWorkspacePathBytes();

  /**
   * <code>string feature_columns = 2;</code>
   */
  java.lang.String getFeatureColumns();
  /**
   * <code>string feature_columns = 2;</code>
   */
  com.google.protobuf.ByteString
      getFeatureColumnsBytes();

  /**
   * <code>string cluster_column = 3;</code>
   */
  java.lang.String getClusterColumn();
  /**
   * <code>string cluster_column = 3;</code>
   */
  com.google.protobuf.ByteString
      getClusterColumnBytes();

  /**
   * <code>bytes centroids = 4;</code>
   */
  com.google.protobuf.ByteString getCentroids();

  /**
   * <code>int32 worker_count = 5;</code>
   */
  int getWorkerCount();
}
