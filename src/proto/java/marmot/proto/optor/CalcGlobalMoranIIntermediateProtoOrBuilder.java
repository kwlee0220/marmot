// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

public interface CalcGlobalMoranIIntermediateProtoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:marmot.proto.optor.CalcGlobalMoranIIntermediateProto)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string dataset = 1;</code>
   */
  java.lang.String getDataset();
  /**
   * <code>string dataset = 1;</code>
   */
  com.google.protobuf.ByteString
      getDatasetBytes();

  /**
   * <code>string geometry_column = 2;</code>
   */
  java.lang.String getGeometryColumn();
  /**
   * <code>string geometry_column = 2;</code>
   */
  com.google.protobuf.ByteString
      getGeometryColumnBytes();

  /**
   * <code>string target_column = 3;</code>
   */
  java.lang.String getTargetColumn();
  /**
   * <code>string target_column = 3;</code>
   */
  com.google.protobuf.ByteString
      getTargetColumnBytes();

  /**
   * <code>double radius = 4;</code>
   */
  double getRadius();

  /**
   * <code>.marmot.proto.optor.LISAWeightProto wtype = 5;</code>
   */
  int getWtypeValue();
  /**
   * <code>.marmot.proto.optor.LISAWeightProto wtype = 5;</code>
   */
  marmot.proto.optor.LISAWeightProto getWtype();

  /**
   * <code>double average = 6;</code>
   */
  double getAverage();
}