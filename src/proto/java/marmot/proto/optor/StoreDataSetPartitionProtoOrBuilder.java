// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

public interface StoreDataSetPartitionProtoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:marmot.proto.optor.StoreDataSetPartitionProto)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string path = 1;</code>
   */
  java.lang.String getPath();
  /**
   * <code>string path = 1;</code>
   */
  com.google.protobuf.ByteString
      getPathBytes();

  /**
   * <code>.marmot.proto.service.StoreDataSetOptionsProto options = 2;</code>
   */
  boolean hasOptions();
  /**
   * <code>.marmot.proto.service.StoreDataSetOptionsProto options = 2;</code>
   */
  marmot.proto.service.StoreDataSetOptionsProto getOptions();
  /**
   * <code>.marmot.proto.service.StoreDataSetOptionsProto options = 2;</code>
   */
  marmot.proto.service.StoreDataSetOptionsProtoOrBuilder getOptionsOrBuilder();
}