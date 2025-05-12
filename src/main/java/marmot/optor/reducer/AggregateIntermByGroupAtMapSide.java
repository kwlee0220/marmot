package marmot.optor.reducer;

import java.util.Map;

import org.slf4j.Logger;

import com.google.common.collect.Maps;

import utils.StopWatch;
import utils.stream.FStream;
import utils.stream.KeyValueFStream;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.TaggedRecordKey;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.AggregateIntermByGroupAtMapSideProto;
import marmot.protobuf.PBUtils;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateIntermByGroupAtMapSide extends AbstractRecordSetFunction
								implements PBSerializable<AggregateIntermByGroupAtMapSideProto> {
	private final MultiColumnKey m_keys;
	private final MultiColumnKey m_tagKeys;
	private final VARIntermediateReducer m_reducer;
	
	private RecordSchema m_valueSchema;
	
	public AggregateIntermByGroupAtMapSide(MultiColumnKey keys, MultiColumnKey tagKeys,
											VARIntermediateReducer reducer) {
		m_keys = keys;
		m_tagKeys = tagKeys;
		m_reducer = reducer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;

		MultiColumnKey grpKeys = MultiColumnKey.concat(m_keys, m_tagKeys);
		RecordSchema keySchema = inputSchema.project(grpKeys.getColumnNames());
		m_valueSchema = inputSchema.complement(grpKeys.getColumnNames());
		m_reducer.initialize(marmot, m_valueSchema);
		
		RecordSchema reducedSchema = m_reducer.getRecordSchema();
		RecordSchema outSchema = RecordSchema.concat(keySchema, reducedSchema);
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	private static class AccumPerGroup {
		private final Record m_accumRecord;
		private int m_accumCount;
		
		AccumPerGroup(Record accumRecord, int accumCount) {
			m_accumRecord = accumRecord;
			m_accumCount = accumCount;
		}
	}
	
	@Override
	public RecordSet apply(RecordSet rset) {
		checkInitialized();
		
		return new Aggregated(this, rset);
	}

	public static AggregateIntermByGroupAtMapSide fromProto(AggregateIntermByGroupAtMapSideProto proto) {
		MultiColumnKey cmpKeyCols = MultiColumnKey.fromString(proto.getKeyColumns());
		MultiColumnKey taggedKeyCols = MultiColumnKey.fromString(proto.getTagColumns());
		VARIntermediateReducer combiner = (VARIntermediateReducer)PBUtils.deserialize(proto.getReducer());
		
		return new AggregateIntermByGroupAtMapSide(cmpKeyCols, taggedKeyCols, combiner);
	}
	
	@Override
	public AggregateIntermByGroupAtMapSideProto toProto() {
		return AggregateIntermByGroupAtMapSideProto.newBuilder()
									.setKeyColumns(m_keys.toString())
									.setTagColumns(m_tagKeys.toString())
									.setReducer(((PBSerializable<?>)m_reducer).serialize())
									.build();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(String.format("%s: ", getClass().getSimpleName()));
		
		builder.append("keys={").append(m_keys).append('}');
		if ( m_tagKeys.length() > 0 ) {
			builder.append(",tags={").append(m_tagKeys).append('}');
		}
		
		String aggrsStr = FStream.of(m_reducer.getValueAggregateAll())
								.map(ValueAggregate::toString)
								.join(",");
		builder.append(",aggrs={").append(aggrsStr);
		builder.append("}]");
		
		return String.format(builder.toString());
	}
	
	class Aggregated extends AbstractRecordSet implements ProgressReportable {
		private final AggregateIntermByGroupAtMapSide m_optor;
		private final RecordSet m_inputRSet;
		
		private RecordSet m_aggregateds = null;
		private Map<TaggedRecordKey,AccumPerGroup> m_accumGroups;
		private long m_count =0;
		protected long m_elapsed;
		private boolean m_finalProgressReported = false;
		
		Aggregated(AggregateIntermByGroupAtMapSide optor, RecordSet input) {
			m_optor = optor;
			m_inputRSet = input;
			m_accumGroups = Maps.newHashMap();
		}

		@Override
		protected void closeInGuard() throws Exception {
			m_inputRSet.close();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_optor.getRecordSchema();
		}
		
		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			if ( m_aggregateds == null ) {
				m_aggregateds = aggregate(m_inputRSet);
				m_inputRSet.closeQuietly();
			}
			
			return m_aggregateds.next(output);
		}
		
		@Override
		public String toString() {
			double velo = m_count / (double)(m_elapsed/1000);
			return String.format("%s: group_count=%,d, record_count=%,d, velo=%.0f/s",
								m_optor, m_accumGroups.size(), m_count, velo);
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			if ( !isClosed() || !m_finalProgressReported ) {
				if ( m_inputRSet instanceof ProgressReportable ) {
					((ProgressReportable)m_inputRSet).reportProgress(logger, elapsed);
				}
				
				m_elapsed = elapsed.getElapsedInMillis();
				logger.info("report: [{}]{}", isClosed() ? "C": "O", toString());
				
				if ( isClosed() ) {
					m_finalProgressReported = true;
				}
			}
		}

		private RecordSet aggregate(RecordSet rset) {
			RecordSchema inputSchema = rset.getRecordSchema();
			int[] m_valueColIdxes = m_valueSchema.streamColumns()
													.map(Column::name)
													.map(inputSchema::getColumn)
													.mapToInt(Column::ordinal)
													.toArray();
			
			Record valueRecord = DefaultRecord.of(m_optor.m_valueSchema);
			
			Record input = DefaultRecord.of(m_inputSchema);
			while ( rset.next(input) ) {
				for ( int i =0; i < m_valueColIdxes.length; ++i ) {
					valueRecord.set(i, input.get(m_valueColIdxes[i]));
				}
				
				TaggedRecordKey key = TaggedRecordKey.from(m_keys, m_tagKeys, input);
				AccumPerGroup group = m_accumGroups.get(key);
				if ( group != null ) {
					m_reducer.reduce(group.m_accumRecord, valueRecord, group.m_accumCount++);
				}
				else {
					Record accumRecord = DefaultRecord.of(m_reducer.getRecordSchema());
					m_reducer.reduce(accumRecord, valueRecord, 0);
					m_accumGroups.put(key, new AccumPerGroup(accumRecord, 1));
				}
				
				++m_count;
			}
			
			return RecordSet.from(getRecordSchema(),
									KeyValueFStream.from(m_accumGroups)
													.map((k, v) -> {
														Record output = DefaultRecord.of(getRecordSchema());
														output.setAll(k.getValues());
														output.setAll(k.length(), v.m_accumRecord.getAll());
														return output;
													}));
		}
	}
}
