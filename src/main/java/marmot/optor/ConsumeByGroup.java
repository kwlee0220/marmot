package marmot.optor;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.mapreduce.MarmotMapOutputKeyColumns.GroupPartitioner;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.reducer.GroupByProtoUtils;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.rset.SimpleKeyedRecordSet;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.ConsumeByGroupProto;
import marmot.proto.optor.GroupByKeyProto;
import marmot.proto.optor.GroupConsumerProto;
import marmot.support.PBSerializable;
import utils.func.FOption;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ConsumeByGroup extends AbstractRecordSetConsumer
							implements MapReduceJobConfigurer,
											PBSerializable<ConsumeByGroupProto> {
	private final MultiColumnKey m_keyCols;
	private final MultiColumnKey m_tagCols;
	private final MultiColumnKey m_orderCols;
	private final RecordSetConsumer m_consumer;
	private final FOption<Integer> m_workerCount;
	private final FOption<RecordSetFunction> m_transform;
	
	private ConsumeByGroup(MultiColumnKey keyCols, MultiColumnKey tagCols,
							MultiColumnKey orderCols, FOption<RecordSetFunction> trans,
							RecordSetConsumer consumer, FOption<Integer> workerCount) {
		m_keyCols = keyCols;
		m_tagCols = tagCols;
		m_orderCols = orderCols;
		m_transform = trans;
		m_consumer = consumer;
		m_workerCount = workerCount;
		
		setLogger(LoggerFactory.getLogger(ConsumeByGroup.class));
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();

		MarmotFileOutputFormat.setRecordSchema(job.getConfiguration(), getOutputRecordSchema());
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputValueClass(NullWritable.class);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		MultiColumnKey grpCols = MultiColumnKey.concat(m_keyCols, m_tagCols);
		RecordSchema valueSchema = inputSchema.complement(grpCols.getColumnNames());
		
		if ( m_transform.isPresent() ) {
			RecordSetFunction trans = m_transform.get();
			
			trans.initialize(marmot, inputSchema);
			valueSchema = trans.getRecordSchema();
		}
		m_consumer.initialize(marmot, valueSchema);

		super.initialize(marmot, inputSchema);
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		
		KeyedRecordSetFactory groups;
		if ( rset instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)rset).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, rset.getRecordSchema(),
															m_keyCols, m_tagCols);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(rset, m_keyCols, m_tagCols,
														m_orderCols, true);
		}
		
		try {
			FOption<KeyedRecordSet> ogroup;
			while ( (ogroup = groups.nextKeyedRecordSet()).isPresent() ) {
				KeyedRecordSet group = ogroup.get();
				
				KeyedRecordSet keyed = group;
				if ( m_transform.isPresent() ) {
					RecordSet output = m_transform.get().apply(group);
					keyed = new SimpleKeyedRecordSet(group.getKey(), group.getKeySchema(), output);
				};
				m_consumer.consume(keyed);
				
				if ( getLogger().isInfoEnabled() ) {
					getLogger().info("done: key={}, {}", group.getKey(), m_consumer);
				}
			}
		}
		finally {
			IOUtils.closeQuietly(groups);
		}
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		checkInitialized();

		MapReduceJoint joint = MapReduceJoint.create();
		
		joint.setMapOutputKey(MarmotMapOutputKeyColumns.fromGroupKey(m_keyCols, m_orderCols));
		joint.setReducerCount(m_workerCount.getOrElse(m_marmot.getDefaultPartitionCount()));
		joint.setPartitionerClass(GroupPartitioner.class);
		
		// reducer chain 설정
		joint.addReducer(this);
		
		return joint;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder("consume_by_group: ");
		
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

	public static ConsumeByGroup fromProto(ConsumeByGroupProto proto) {
		GroupByKeyProto grouper = proto.getGrouper();
		
		MultiColumnKey cmpCols = MultiColumnKey.fromString(grouper.getCompareColumns());
		
		MultiColumnKey tagCols = MultiColumnKey.EMPTY;
		switch ( grouper.getOptionalTagColumnsCase() ) {
			case TAG_COLUMNS:
				tagCols = MultiColumnKey.fromString(grouper.getTagColumns());
				break;
			case OPTIONALTAGCOLUMNS_NOT_SET:
				break;
		}
		
		MultiColumnKey orderCols = MultiColumnKey.EMPTY;
		switch ( grouper.getOptionalOrderColumnsCase() ) {
			case ORDER_COLUMNS:
				orderCols = MultiColumnKey.fromString(grouper.getOrderColumns());
				break;
			case OPTIONALORDERCOLUMNS_NOT_SET:
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
		
		FOption<RecordSetFunction> trans = FOption.empty();
		switch ( proto.getOptionalTransformCase() ) {
			case TRANSFORM:
				trans = FOption.of(GroupByProtoUtils.fromProto(proto.getTransform()));
				break;
			case OPTIONALTRANSFORM_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		RecordSetConsumer consumer = GroupByProtoUtils.fromProto(proto.getConsumer());
		
		return new ConsumeByGroup(cmpCols, tagCols, orderCols, trans, consumer, nworkers);
	}

	@Override
	public ConsumeByGroupProto toProto() {
		GroupByKeyProto.Builder grpBuilder = GroupByKeyProto.newBuilder()
												.setCompareColumns(m_keyCols.toString());
		if ( m_tagCols.length() > 0 ) {
			grpBuilder.setTagColumns(m_tagCols.toString());
		}
		if ( m_orderCols.length() > 0 ) {
			grpBuilder.setOrderColumns(m_orderCols.toString());
		}
		m_workerCount.ifPresent(c -> grpBuilder.setGroupWorkerCount(c));
		GroupByKeyProto grouper = grpBuilder.build();
		
		
		GroupConsumerProto proto = GroupByProtoUtils.toProto(m_consumer);
		return ConsumeByGroupProto.newBuilder()
									.setGrouper(grouper)
									.setConsumer(proto)
									.build();
	}
}
