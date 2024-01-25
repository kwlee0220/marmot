package marmot.module.geo;

import java.util.List;

import org.apache.hadoop.mapreduce.ReduceContext;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.analysis.module.geo.FeatureVector;
import marmot.analysis.module.geo.FeatureVectorHandle;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ComputeClusterCenters extends AbstractRecordSetFunction {
	private final List<String> m_featureColNames;
	private final String m_clusterColName;
	
	public ComputeClusterCenters(List<String> featureColNames, String clusterColName) {
		m_featureColNames = featureColNames;
		m_clusterColName = clusterColName;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema.Builder builder = RecordSchema.builder();
		m_featureColNames.stream()
						.map(name -> inputSchema.getColumn(name))
						.forEach(col -> builder.addColumn(col.name(), col.type()));
		RecordSchema outSchema = builder.addColumn(m_clusterColName, DataType.INT).build();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		MultiColumnKey key = MultiColumnKey.fromString(m_clusterColName);
		MultiColumnKey taggeds = MultiColumnKey.EMPTY;
		
		KeyedRecordSetFactory groups;
		if ( input instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)input).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, input.getRecordSchema(),
															key, taggeds);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(input, key, taggeds,
														MultiColumnKey.EMPTY, false);
		}
		
		return new Relocateds(this, groups);
	}
	
	static class Relocateds extends AbstractRecordSet {
		private final ComputeClusterCenters m_optor;
		private final KeyedRecordSetFactory m_rsetFact;
		private final Record m_inputRecord;
		private final FeatureVectorHandle m_handle;
		private final String m_clusterColName;
		
		Relocateds(ComputeClusterCenters compute, KeyedRecordSetFactory rsetFact) {
			m_optor = compute;
			m_rsetFact = rsetFact;
			m_clusterColName = compute.m_clusterColName;
			m_handle = new FeatureVectorHandle(compute.m_featureColNames);
			
			RecordSchema inputSchema = m_rsetFact.getRecordSchema();
			m_inputRecord = DefaultRecord.of(inputSchema);
		}

		@Override
		protected void closeInGuard() {
			m_rsetFact.closeQuietly();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_optor.getRecordSchema();
		}
		
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			RecordSet cluster = m_rsetFact.nextKeyedRecordSet().getOrNull();
			if ( cluster == null ) {
				return false;
			}
			
			int clusterIdx = -1;
			List<FeatureVector> features = Lists.newArrayList();
			try {
				while ( cluster.next(m_inputRecord) ) {
					if ( clusterIdx < 0 ) {
						clusterIdx = m_inputRecord.getInt(m_clusterColName);
					}
					features.add(m_handle.take(m_inputRecord));
				}
			}
			finally {
				cluster.closeQuietly();
			}
				
			FeatureVector mean = FeatureVector.calcMean(features);
			m_handle.put(mean, record);
			record.set(m_clusterColName, clusterIdx);
			
			return true;
		}
	}
}