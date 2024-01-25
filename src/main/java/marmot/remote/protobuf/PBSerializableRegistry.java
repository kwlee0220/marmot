package marmot.remote.protobuf;

import marmot.geowave.LoadGHdfsFile;
import marmot.geowave.StoreAsGHdfs;
import marmot.mapreduce.IntermediateTerminal;
import marmot.mapreduce.MapOutputWriter;
import marmot.optor.AssignUid;
import marmot.optor.ClusterChronicles;
import marmot.optor.CollectToArrayColumn;
import marmot.optor.ConsumeByGroup;
import marmot.optor.DefineColumn;
import marmot.optor.Distinct;
import marmot.optor.Drop;
import marmot.optor.Expand;
import marmot.optor.HashJoin;
import marmot.optor.LoadCustomTextFile;
import marmot.optor.LoadDataSet;
import marmot.optor.LoadJdbcTable;
import marmot.optor.LoadMarmotFile;
import marmot.optor.LoadTextFile;
import marmot.optor.LoadWholeFile;
import marmot.optor.ParseCsvText;
import marmot.optor.PickTopK;
import marmot.optor.Project;
import marmot.optor.Rank;
import marmot.optor.Reduce;
import marmot.optor.Sample;
import marmot.optor.ScriptFilter;
import marmot.optor.Shard;
import marmot.optor.Sort;
import marmot.optor.StoreAndReload;
import marmot.optor.StoreAsCsv;
import marmot.optor.StoreAsHeapfile;
import marmot.optor.StoreDataSet;
import marmot.optor.StoreDataSetPartition;
import marmot.optor.StoreIntoJdbcTable;
import marmot.optor.StoreIntoKafkaTopic;
import marmot.optor.StoreKeyedDataSet;
import marmot.optor.Take;
import marmot.optor.Tee;
import marmot.optor.TransformByGroup;
import marmot.optor.Update;
import marmot.optor.geo.AssignSquareGridCell;
import marmot.optor.geo.BreakLineString;
import marmot.optor.geo.Dissolve;
import marmot.optor.geo.FlattenGeometry;
import marmot.optor.geo.LoadHexagonGridFile;
import marmot.optor.geo.LoadSpatialIndexJoin;
import marmot.optor.geo.LoadSquareGridFile;
import marmot.optor.geo.QueryDataSet;
import marmot.optor.geo.ToXYCoordinates;
import marmot.optor.geo.ValidateGeometry;
import marmot.optor.geo.advanced.CalcGetisOrdGi;
import marmot.optor.geo.advanced.CalcLocalMoransI;
import marmot.optor.geo.advanced.CreateThumbnail;
import marmot.optor.geo.advanced.EstimateIDW;
import marmot.optor.geo.advanced.EstimateKernelDensity;
import marmot.optor.geo.advanced.LoadGetisOrdGi;
import marmot.optor.geo.advanced.LoadLocalMoransI;
import marmot.optor.geo.advanced.SpatialInterpolation;
import marmot.optor.geo.arc.ArcClip;
import marmot.optor.geo.arc.ArcSpatialJoin;
import marmot.optor.geo.arc.ArcUnionPhase1;
import marmot.optor.geo.cluster.AttachQuadKey;
import marmot.optor.geo.cluster.ClusterSpatially;
import marmot.optor.geo.cluster.MBRTaggedOpaqueTransform;
import marmot.optor.geo.cluster.SplitQuadSpace;
import marmot.optor.geo.cluster.StoreSpatialClusterPack;
import marmot.optor.geo.filter.BinarySpatialIntersects;
import marmot.optor.geo.filter.DropEmptyGeometry;
import marmot.optor.geo.filter.FilterSpatially;
import marmot.optor.geo.filter.WithinDistance;
import marmot.optor.geo.index.LoadSpatialGlobalIndex;
import marmot.optor.geo.index.LoadSpatialIndexedFile;
import marmot.optor.geo.join.SpatialBlockJoin;
import marmot.optor.geo.join.SpatialDifferenceJoin;
import marmot.optor.geo.join.SpatialIntersectionJoin;
import marmot.optor.geo.join.SpatialKnnInnerJoin;
import marmot.optor.geo.join.SpatialKnnOuterJoin;
import marmot.optor.geo.join.SpatialOuterJoin;
import marmot.optor.geo.join.SpatialReduceJoin;
import marmot.optor.geo.join.SpatialSemiJoin;
import marmot.optor.geo.transform.BinarySpatialIntersection;
import marmot.optor.geo.transform.BinarySpatialUnion;
import marmot.optor.geo.transform.BufferTransform;
import marmot.optor.geo.transform.CentroidTransform;
import marmot.optor.geo.transform.ReduceGeometryPrecision;
import marmot.optor.geo.transform.ToGeometryPoint;
import marmot.optor.geo.transform.TransformSrid;
import marmot.optor.geo.transform.UnarySpatialIntersection;
import marmot.optor.join.InnerJoinAtMapSide;
import marmot.optor.join.JoinPartitions;
import marmot.optor.join.LoadHashJoin;
import marmot.optor.join.TagJoinKeyColumns;
import marmot.optor.reducer.CombineableScriptRecordSetReducer;
import marmot.optor.reducer.FinalizeIntermByGroup;
import marmot.optor.reducer.ListReducer;
import marmot.optor.reducer.ProduceIntermByGroup;
import marmot.optor.reducer.PutSideBySide;
import marmot.optor.reducer.ReduceIntermByGroup;
import marmot.optor.reducer.RunPlanReducer;
import marmot.optor.reducer.TakeReducer;
import marmot.optor.reducer.VARIntermediateFinalizer;
import marmot.optor.reducer.VARIntermediateProducer;
import marmot.optor.reducer.VARIntermediateReducer;
import marmot.optor.reducer.ValueAggregateReducer;
import marmot.optor.support.ScriptRecordTransform;
import marmot.proto.optor.*;
import marmot.protobuf.ProtoBufActivator;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBSerializableRegistry {
	public static void bindAll() {
		ProtoBufActivator.bind(ScriptFilterProto.class, ScriptFilter.class);
		ProtoBufActivator.bind(ScriptTransformProto.class, ScriptRecordTransform.class);
		
		ProtoBufActivator.bind(LoadMarmotFileProto.class, LoadMarmotFile.class);
		ProtoBufActivator.bind(LoadTextFileProto.class, LoadTextFile.class);
		ProtoBufActivator.bind(LoadCustomTextFileProto.class, LoadCustomTextFile.class);
		ProtoBufActivator.bind(LoadDataSetProto.class, LoadDataSet.class);
		ProtoBufActivator.bind(QueryDataSetProto.class, QueryDataSet.class);
		ProtoBufActivator.bind(LoadSpatialClusteredFileProto.class, LoadSpatialIndexedFile.class);
		ProtoBufActivator.bind(LoadSpatialGlobalIndexProto.class, LoadSpatialGlobalIndex.class);
		ProtoBufActivator.bind(LoadSquareGridFileProto.class, LoadSquareGridFile.class);
		ProtoBufActivator.bind(LoadHexagonGridFileProto.class, LoadHexagonGridFile.class);
		ProtoBufActivator.bind(LoadJdbcTableProto.class, LoadJdbcTable.class);
		ProtoBufActivator.bind(LoadWholeFileProto.class, LoadWholeFile.class);
		ProtoBufActivator.bind(LoadGHdfsFileProto.class, LoadGHdfsFile.class);

		ProtoBufActivator.bind(StoreDataSetProto.class, StoreDataSet.class);
		ProtoBufActivator.bind(StoreDataSetPartitionProto.class, StoreDataSetPartition.class);
		ProtoBufActivator.bind(StoreAsHeapfileProto.class, StoreAsHeapfile.class);
		ProtoBufActivator.bind(StoreAsCsvProto.class, StoreAsCsv.class);
		ProtoBufActivator.bind(StoreAsGHdfsProto.class, StoreAsGHdfs.class);
		ProtoBufActivator.bind(StoreIntoJdbcTableProto.class, StoreIntoJdbcTable.class);
		ProtoBufActivator.bind(StoreIntoKafkaTopicProto.class, StoreIntoKafkaTopic.class);
		ProtoBufActivator.bind(TeeProto.class, Tee.class);
		ProtoBufActivator.bind(BuildThumbnailProto.class, CreateThumbnail.class);

		ProtoBufActivator.bind(TransformByGroupProto.class, TransformByGroup.class);
		ProtoBufActivator.bind(ConsumeByGroupProto.class, ConsumeByGroup.class);
		ProtoBufActivator.bind(RunPlanProto.class, RunPlanReducer.class);
		ProtoBufActivator.bind(ProjectProto.class, Project.class);
		ProtoBufActivator.bind(UpdateProto.class, Update.class);
		ProtoBufActivator.bind(ExpandProto.class, Expand.class);
		ProtoBufActivator.bind(DefineColumnProto.class, DefineColumn.class);
		ProtoBufActivator.bind(CollectToArrayColumnProto.class, CollectToArrayColumn.class);
		ProtoBufActivator.bind(TakeProto.class, Take.class);
		ProtoBufActivator.bind(DropProto.class, Drop.class);
		ProtoBufActivator.bind(AssignUidProto.class, AssignUid.class);
		ProtoBufActivator.bind(SampleProto.class, Sample.class);
		ProtoBufActivator.bind(SortProto.class, Sort.class);
		ProtoBufActivator.bind(RankProto.class, Rank.class);
		ProtoBufActivator.bind(PickTopKProto.class, PickTopK.class);
		ProtoBufActivator.bind(ParseCsvProto.class, ParseCsvText.class);
		ProtoBufActivator.bind(ShardProto.class, Shard.class);
		ProtoBufActivator.bind(StoreAndReloadProto.class, StoreAndReload.class);	// ???
		ProtoBufActivator.bind(ClusterChroniclesProto.class, ClusterChronicles.class);

		ProtoBufActivator.bind(LoadHashJoinProto.class, LoadHashJoin.class);
		ProtoBufActivator.bind(HashJoinProto.class, HashJoin.class);
		ProtoBufActivator.bind(InnerJoinAtMapSideProto.class, InnerJoinAtMapSide.class);
		ProtoBufActivator.bind(TagJoinKeyColumnsProto.class, TagJoinKeyColumns.class);
		ProtoBufActivator.bind(JoinPartitionsProto.class, JoinPartitions.class);
		
		ProtoBufActivator.bind(ReducerProto.class, Reduce.class);
		ProtoBufActivator.bind(ProduceByGroupProto.class, ProduceIntermByGroup.class);
		ProtoBufActivator.bind(CombineByGroupProto.class, ReduceIntermByGroup.class);
		ProtoBufActivator.bind(FinalizeByGroupProto.class, FinalizeIntermByGroup.class);
		ProtoBufActivator.bind(ScriptRecordSetReducerProto.class, CombineableScriptRecordSetReducer.class);

		ProtoBufActivator.bind(ValueAggregateReducersProto.class, ValueAggregateReducer.class);
		ProtoBufActivator.bind(VARIntermediateProducerProto.class, VARIntermediateProducer.class);
		ProtoBufActivator.bind(VARIntermediateCombinerProto.class, VARIntermediateReducer.class);
		ProtoBufActivator.bind(VARIntermediateFinalizerProto.class, VARIntermediateFinalizer.class);
		ProtoBufActivator.bind(DistinctProto.class, Distinct.class);
		ProtoBufActivator.bind(TakeReducerProto.class, TakeReducer.class);
		ProtoBufActivator.bind(ListReducerProto.class, ListReducer.class);
		ProtoBufActivator.bind(TakeCombinerProto.class, TakeReducer.TakeRecords.class);
		ProtoBufActivator.bind(PutSideBySideProto.class, PutSideBySide.class);
		ProtoBufActivator.bind(StoreKeyedDataSetProto.class, StoreKeyedDataSet.class);

		ProtoBufActivator.bind(DropEmptyGeometryProto.class, DropEmptyGeometry.class);
		ProtoBufActivator.bind(WithinDistanceProto.class, WithinDistance.class);
		ProtoBufActivator.bind(WithinDistanceProto.class, WithinDistance.class);
		ProtoBufActivator.bind(FilterSpatiallyProto.class, FilterSpatially.class);
		ProtoBufActivator.bind(BinarySpatialIntersectsProto.class, BinarySpatialIntersects.class);

		ProtoBufActivator.bind(LoadSpatialIndexJoinProto.class, LoadSpatialIndexJoin.class);
		ProtoBufActivator.bind(SpatialBlockJoinProto.class, SpatialBlockJoin.class);
		ProtoBufActivator.bind(SpatialSemiJoinProto.class, SpatialSemiJoin.class);
		ProtoBufActivator.bind(SpatialOuterJoinProto.class, SpatialOuterJoin.class);
		ProtoBufActivator.bind(SpatialKnnInnerJoinProto.class, SpatialKnnInnerJoin.class);
		ProtoBufActivator.bind(SpatialKnnOuterJoinProto.class, SpatialKnnOuterJoin.class);
		ProtoBufActivator.bind(ArcClipProto.class, ArcClip.class);
		ProtoBufActivator.bind(SpatialIntersectionJoinProto.class, SpatialIntersectionJoin.class);
		ProtoBufActivator.bind(SpatialDifferenceJoinProto.class, SpatialDifferenceJoin.class);
		ProtoBufActivator.bind(SpatialReduceJoinProto.class, SpatialReduceJoin.class);
		ProtoBufActivator.bind(ArcSpatialJoinProto.class, ArcSpatialJoin.class);
		ProtoBufActivator.bind(ArcUnionPhase1Proto.class, ArcUnionPhase1.class);
		
		ProtoBufActivator.bind(BufferTransformProto.class, BufferTransform.class);
		ProtoBufActivator.bind(CentroidTransformProto.class, CentroidTransform.class);
		ProtoBufActivator.bind(ReduceGeometryPrecisionProto.class, ReduceGeometryPrecision.class);
		ProtoBufActivator.bind(TransformCrsProto.class, TransformSrid.class);
		ProtoBufActivator.bind(UnarySpatialIntersectionProto.class, UnarySpatialIntersection.class);
		ProtoBufActivator.bind(BinarySpatialUnionProto.class, BinarySpatialUnion.class);
		ProtoBufActivator.bind(BinarySpatialIntersectionProto.class, BinarySpatialIntersection.class);
		ProtoBufActivator.bind(ToGeometryPointProto.class, ToGeometryPoint.class);
		ProtoBufActivator.bind(ToXYCoordinatesProto.class, ToXYCoordinates.class);
		
		ProtoBufActivator.bind(DissolveProto.class, Dissolve.class);
		ProtoBufActivator.bind(AssignSquareGridCellProto.class, AssignSquareGridCell.class);
		ProtoBufActivator.bind(FlattenGeometryProto.class, FlattenGeometry.class);
		ProtoBufActivator.bind(BreakLineStringProto.class, BreakLineString.class);
		ProtoBufActivator.bind(ValidateGeometryProto.class, ValidateGeometry.class);

		ProtoBufActivator.bind(ClusterSpatiallyProto.class, ClusterSpatially.class);
		ProtoBufActivator.bind(AttachQuadKeyProto.class, AttachQuadKey.class);
		ProtoBufActivator.bind(MBRTaggedOpaqueTransformProto.class, MBRTaggedOpaqueTransform.class);
		ProtoBufActivator.bind(SplitQuadSpaceProto.class, SplitQuadSpace.class);
		ProtoBufActivator.bind(StoreSpatialClusterPackProto.class, StoreSpatialClusterPack.class);

		ProtoBufActivator.bind(InterpolateSpatiallyProto.class, SpatialInterpolation.class);
		ProtoBufActivator.bind(EstimateIDWProto.class, EstimateIDW.class);
		ProtoBufActivator.bind(EstimateKernelDensityProto.class, EstimateKernelDensity.class);
		ProtoBufActivator.bind(LoadGetisOrdGiProto.class, LoadGetisOrdGi.class);
		ProtoBufActivator.bind(CalcGetisOrdGiProto.class, CalcGetisOrdGi.class);
		ProtoBufActivator.bind(LoadLocalMoransIProto.class, LoadLocalMoransI.class);
		ProtoBufActivator.bind(CalcLocalMoransIProto.class, CalcLocalMoransI.class);
		
		ProtoBufActivator.bind(MapOutputWriterProto.class, MapOutputWriter.class);
		ProtoBufActivator.bind(IntermediateTerminalProto.class, IntermediateTerminal.class);
	}
}
