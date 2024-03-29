// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: marmot.io.proto

package marmot.proto.io;

public interface GroupKeyValueProtoOrBuilder extends
    // @@protoc_insertion_point(interface_extends:marmot.proto.io.GroupKeyValueProto)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int32 key_values_count = 1;</code>
   */
  int getKeyValuesCount();

  /**
   * <code>int32 tag_values_count = 2;</code>
   */
  int getTagValuesCount();

  /**
   * <code>int32 order_values_count = 3;</code>
   */
  int getOrderValuesCount();

  /**
   * <code>repeated .marmot.proto.ValueProto value = 4;</code>
   */
  java.util.List<marmot.proto.ValueProto> 
      getValueList();
  /**
   * <code>repeated .marmot.proto.ValueProto value = 4;</code>
   */
  marmot.proto.ValueProto getValue(int index);
  /**
   * <code>repeated .marmot.proto.ValueProto value = 4;</code>
   */
  int getValueCount();
  /**
   * <code>repeated .marmot.proto.ValueProto value = 4;</code>
   */
  java.util.List<? extends marmot.proto.ValueProtoOrBuilder> 
      getValueOrBuilderList();
  /**
   * <code>repeated .marmot.proto.ValueProto value = 4;</code>
   */
  marmot.proto.ValueProtoOrBuilder getValueOrBuilder(
      int index);

  /**
   * <code>repeated .marmot.proto.io.SortOrderProto sort_order = 5;</code>
   */
  java.util.List<marmot.proto.io.SortOrderProto> getSortOrderList();
  /**
   * <code>repeated .marmot.proto.io.SortOrderProto sort_order = 5;</code>
   */
  int getSortOrderCount();
  /**
   * <code>repeated .marmot.proto.io.SortOrderProto sort_order = 5;</code>
   */
  marmot.proto.io.SortOrderProto getSortOrder(int index);
  /**
   * <code>repeated .marmot.proto.io.SortOrderProto sort_order = 5;</code>
   */
  java.util.List<java.lang.Integer>
  getSortOrderValueList();
  /**
   * <code>repeated .marmot.proto.io.SortOrderProto sort_order = 5;</code>
   */
  int getSortOrderValue(int index);

  /**
   * <code>repeated .marmot.proto.io.NullsOrderProto nulls_order = 6;</code>
   */
  java.util.List<marmot.proto.io.NullsOrderProto> getNullsOrderList();
  /**
   * <code>repeated .marmot.proto.io.NullsOrderProto nulls_order = 6;</code>
   */
  int getNullsOrderCount();
  /**
   * <code>repeated .marmot.proto.io.NullsOrderProto nulls_order = 6;</code>
   */
  marmot.proto.io.NullsOrderProto getNullsOrder(int index);
  /**
   * <code>repeated .marmot.proto.io.NullsOrderProto nulls_order = 6;</code>
   */
  java.util.List<java.lang.Integer>
  getNullsOrderValueList();
  /**
   * <code>repeated .marmot.proto.io.NullsOrderProto nulls_order = 6;</code>
   */
  int getNullsOrderValue(int index);
}
