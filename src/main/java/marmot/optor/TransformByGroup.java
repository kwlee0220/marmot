package marmot.optor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.ReduceContext;

import marmot.Column;
import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.mapreduce.MarmotMapOutputKeyColumns.GroupPartitioner;
import marmot.mapreduce.MarmotMapOutputKeyColumns.ValueBasedGroupPartitioner;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.reducer.AggregateIntermByGroupAtMapSide;
import marmot.optor.reducer.CombineableRecordSetReducer;
import marmot.optor.reducer.FinalizeIntermByGroup;
import marmot.optor.reducer.GroupByProtoUtils;
import marmot.optor.reducer.ProduceIntermByGroup;
import marmot.optor.reducer.ReduceIntermByGroup;
import marmot.optor.reducer.TakeNByGroupAtMapSide;
import marmot.optor.reducer.TakeReducer;
import marmot.optor.reducer.ValueAggregateReducer;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.rset.KeyedGroupTransformedRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.GroupByKeyProto;
import marmot.proto.optor.TransformByGroupProto;
import marmot.support.PBSerializable;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformByGroup extends AbstractRecordSetFunction
								implements PBSerializable<TransformByGroupProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_orderCols;
	private final MultiColumnKey m_grpKeyCols;
	private final RecordSetFunction m_func;
	private final FOption<String> m_partCol;
	private final FOption<Integer> m_workerCount;
	
	private TransformByGroup(MultiColumnKey keyCols, MultiColumnKey tagCols,
							MultiColumnKey orderCols, RecordSetFunction func,
							FOption<String> partCol, FOption<Integer> workerCount) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_orderCols = orderCols;
		m_grpKeyCols = MultiColumnKey.concat(m_keyCols, m_tagCols);
		m_func = func;
		m_partCol = partCol;
		m_workerCount = workerCount;
	}

	@Override
	public boolean isStreamable() {
		return false;
	}
	
	public MultiColumnKey getKeyColumns() {
		return m_keyCols;
	}
	
	public MultiColumnKey getTagColumns() {
		return m_tagCols;
	}
	
	public MultiColumnKey getGroupKeyColumns() {
		return m_grpKeyCols;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema valueSchema = inputSchema.complement(m_grpKeyCols.getColumnNames());
		m_func.initialize(marmot, valueSchema);
		valueSchema = m_func.getRecordSchema();
		
		RecordSchema keySchema = inputSchema.project(m_grpKeyCols.getColumnNames());
		RecordSchema outputSchema = RecordSchema.concat(keySchema, valueSchema);
		setInitialized(marmot, inputSchema, outputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		KeyedRecordSetFactory groups;
		if ( input instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)input).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, input.getRecordSchema(),
															m_keyCols, m_tagCols);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(input, m_keyCols, m_tagCols,
														m_orderCols, true);
		}

		return new KeyedGroupTransformedRecordSet(toString(), groups, m_func);
	}

	@Override
	public MapReduceJoint getMapReduceJoint(final RecordSchema inputSchema) {
		checkInitialized();

		MapReduceJoint joint = MapReduceJoint.create();
		
		joint.setMapOutputKey(MarmotMapOutputKeyColumns.fromGroupKey(m_keyCols, m_orderCols));
		joint.setReducerCount(m_workerCount.getOrElse(m_marmot.getDefaultPartitionCount()));
		m_partCol.ifPresent(col -> {
						Configuration conf = getMarmotCore().getHadoopConfiguration();
						int colIdx = inputSchema.findColumn(col).map(Column::ordinal).getOrElse(-1);
						if ( colIdx < 0 ) {
							throw new IllegalArgumentException("invalid partition column: " + col);
						}
						ValueBasedGroupPartitioner.setPartitionColumnIndex(conf, colIdx);
						joint.setPartitionerClass(ValueBasedGroupPartitioner.class);
					})
					.ifAbsent(() -> joint.setPartitionerClass(GroupPartitioner.class));
		
		if ( m_func instanceof ValueAggregateReducer ) {
			ValueAggregateReducer combineable = (ValueAggregateReducer)m_func;
			
			ProduceIntermByGroup produce = new ProduceIntermByGroup(m_keyCols, m_tagCols,
															combineable.newIntermediateProducer());
			joint.addMapper(produce);
			
			// mapper 단계에서 group 별로 1차 aggregation을 수행함
			AggregateIntermByGroupAtMapSide intraPartReduce
					= new AggregateIntermByGroupAtMapSide(m_keyCols, m_tagCols,
															combineable.newIntermediateReducer());
			joint.addMapper(intraPartReduce);
			
			// reducer 단계에서 group별로 combine을 수행하게 함
			ReduceIntermByGroup reduce = new ReduceIntermByGroup(m_keyCols, m_tagCols, m_orderCols, 
																combineable.newIntermediateReducer());
			joint.addReducer(reduce);
			
			// 누적된 임시 데이터를 최종 결과를 변경시킴
			FinalizeIntermByGroup finalize = new FinalizeIntermByGroup(m_keyCols, m_tagCols, 
																combineable.newIntermediateFinalizer());
			joint.addReducer(finalize);
		}
		else if ( m_func instanceof TakeReducer ) {
			TakeReducer reducer = (TakeReducer)m_func;
			
			// mapper 단계에서 group 별로 1차 aggregation을 수행함
			TakeNByGroupAtMapSide takeAtMapSide
									= new TakeNByGroupAtMapSide(m_keyCols, m_tagCols, m_orderCols,
																reducer.getTakeCount());
			joint.addMapper(takeAtMapSide);
			
			// reducer 단계에서 group별로 combine을 수행하게 함
			ReduceIntermByGroup reduce = new ReduceIntermByGroup(m_keyCols, m_tagCols, m_orderCols, 
																reducer.newIntermediateReducer());
			joint.addReducer(reduce);
		}
		else if ( m_func instanceof CombineableRecordSetReducer ) {
			CombineableRecordSetReducer combineable = (CombineableRecordSetReducer)m_func;
			
			ProduceIntermByGroup produce = new ProduceIntermByGroup(m_keyCols, m_tagCols,
															combineable.newIntermediateProducer());
			joint.addMapper(produce);
			
			ReduceIntermByGroup combine = new ReduceIntermByGroup(m_keyCols, m_tagCols, m_orderCols, 
																combineable.newIntermediateReducer());
			joint.addCombiner(combine);
			
			ReduceIntermByGroup reduce = new ReduceIntermByGroup(m_keyCols, m_tagCols, m_orderCols, 
																combineable.newIntermediateReducer());
			joint.addReducer(reduce);
			
			FinalizeIntermByGroup finalize = new FinalizeIntermByGroup(m_keyCols, m_tagCols, 
																combineable.newIntermediateFinalizer());
			joint.addReducer(finalize);
		}
		else {
			// reducer chain 설정
			TransformByGroup apply = new TransformByGroup(m_keyCols, m_tagCols,
														m_orderCols, m_func, m_partCol, FOption.empty());
			apply.initialize(m_marmot, inputSchema);
			joint.addReducer(apply);
		}
		
		return joint;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("TransformByGroup: ");
		
		builder.append("keys={").append(m_keyCols).append('}');
		
		if ( m_tagCols.length() > 0 ) {
			builder.append(",tags={").append(m_tagCols).append('}');
		}
		
		if ( m_orderCols.length() > 0 ) {
			builder.append(",orders={").append(m_orderCols).append('}');
		}
		builder.append("]");
		
		return String.format(builder.toString());
	}

	public static TransformByGroup fromProto(TransformByGroupProto proto) {
		GroupByKeyProto grouper = proto.getGrouper();
		
		FOption<String> tagCols = FOption.empty();
		switch ( grouper.getOptionalTagColumnsCase() ) {
			case TAG_COLUMNS:
				tagCols = FOption.of(grouper.getTagColumns());
				break;
			case OPTIONALTAGCOLUMNS_NOT_SET:
				break;
		}
		
		FOption<String> orderCols = FOption.empty();
		switch ( grouper.getOptionalOrderColumnsCase() ) {
			case ORDER_COLUMNS:
				orderCols = FOption.of(grouper.getOrderColumns());
				break;
			case OPTIONALORDERCOLUMNS_NOT_SET:
				break;
		}
		
		FOption<String> partCol = FOption.empty();
		switch ( grouper.getOptionalPartitionColumnCase() ) {
			case PARTITION_COLUMN:
				partCol = FOption.of(grouper.getPartitionColumn());
				break;
			case OPTIONALPARTITIONCOLUMN_NOT_SET:
				break;
		}
		
		FOption<Integer> nworkers = FOption.empty();
		switch ( grouper.getOptionalGroupWorkerCountCase() ) {
			case GROUP_WORKER_COUNT:
				nworkers = FOption.of(grouper.getGroupWorkerCount());
				break;
			case OPTIONALGROUPWORKERCOUNT_NOT_SET:
				break;
		}
		
		RecordSetFunction func = GroupByProtoUtils.fromProto(proto.getTransform());
		
		return TransformByGroup.builder()
								.keyColumns(grouper.getCompareColumns())
								.tagColumns(tagCols)
								.orderColumns(orderCols)
								.transform(func)
								.partitionColumn(partCol)
								.workerCount(nworkers)
								.build();
	}
	
	@Override
	public TransformByGroupProto toProto() {
		GroupByKeyProto.Builder grpBuilder = GroupByKeyProto.newBuilder()
													.setCompareColumns(m_keyCols.toString())
													.setTagColumns(m_tagCols.toString())
													.setOrderColumns(m_orderCols.toString());
		m_workerCount.ifPresent(c -> grpBuilder.setGroupWorkerCount(c));
		GroupByKeyProto grouper = grpBuilder.build();
		
		return TransformByGroupProto.newBuilder()
									.setGrouper(grouper)
									.setTransform(GroupByProtoUtils.toProto(m_func))
									.build();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private MultiColumnKey m_keyCols;
		private MultiColumnKey m_tagCols = MultiColumnKey.EMPTY;
		private MultiColumnKey m_orderCols = MultiColumnKey.EMPTY;
		private RecordSetFunction m_func;
		private FOption<String> m_partCol = FOption.empty();
		private FOption<Integer> m_workerCount = FOption.empty();
		
		public TransformByGroup build() {
			return new TransformByGroup(m_keyCols, m_tagCols, m_orderCols, m_func, m_partCol,
										m_workerCount);
		}
		
		public Builder keyColumns(String colNames) {
			m_keyCols = MultiColumnKey.fromString(colNames);
			return this;
		}
		
		public Builder tagColumns(FOption<String> tagCols) {
			m_tagCols = tagCols.map(MultiColumnKey::fromString)
								.getOrElse(MultiColumnKey.EMPTY);
			return this;
		}
		
		public Builder orderColumns(FOption<String> orderCols) {
			m_orderCols = orderCols.map(MultiColumnKey::fromString)
									.getOrElse(MultiColumnKey.EMPTY);
			return this;
		}
		
		public Builder transform(RecordSetFunction func) {
			m_func = func;
			return this;
		}
		
		public Builder partitionColumn(FOption<String> partCol) {
			m_partCol = partCol;
			return this;
		}
		
		public Builder workerCount(int count) {
			m_workerCount = count > 0 ? FOption.of(count) : FOption.empty();
			return this;
		}
		
		public Builder workerCount(FOption<Integer> count) {
			m_workerCount = count;
			return this;
		}
	}
}
