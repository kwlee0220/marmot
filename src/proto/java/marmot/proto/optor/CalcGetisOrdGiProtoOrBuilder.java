// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.optor.internal.proto

package marmot.proto.optor;

public interface CalcGetisOrdGiProtoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:marmot.proto.optor.CalcGetisOrdGiProto)
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
   * <code>string value_column = 3;</code>
   */
  java.lang.String getValueColumn();
  /**
   * <code>string value_column = 3;</code>
   */
  com.google.protobuf.ByteString
      getValueColumnBytes();

  /**
   * <code>double radius = 4;</code>
   */
  double getRadius();

  /**
   * <code>.marmot.proto.optor.LISAWeightProto weight_type = 5;</code>
   */
  int getWeightTypeValue();
  /**
   * <code>.marmot.proto.optor.LISAWeightProto weight_type = 5;</code>
   */
  marmot.proto.optor.LISAWeightProto getWeightType();

  /**
   * <code>.marmot.proto.optor.LISAParametersProto parameters = 6;</code>
   */
  boolean hasParameters();
  /**
   * <code>.marmot.proto.optor.LISAParametersProto parameters = 6;</code>
   */
  marmot.proto.optor.LISAParametersProto getParameters();
  /**
   * <code>.marmot.proto.optor.LISAParametersProto parameters = 6;</code>
   */
  marmot.proto.optor.LISAParametersProtoOrBuilder getParametersOrBuilder();
}
