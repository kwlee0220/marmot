package marmot.optor.reducer;

import org.slf4j.Logger;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.AggregateIntermAtMapSideProto;
import marmot.protobuf.PBUtils;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggregateIntermAtMapSide extends AbstractRecordSetFunction
								implements PBSerializable<AggregateIntermAtMapSideProto> {
	private final VARIntermediateReducer m_reducer;
	
	public AggregateIntermAtMapSide(VARIntermediateReducer reducer) {
		m_reducer = reducer;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_reducer.initialize(marmot, inputSchema);
		RecordSchema reducedSchema = m_reducer.getRecordSchema();
		setInitialized(marmot, inputSchema, reducedSchema);
	}

	@Override
	public RecordSet apply(RecordSet rset) {
		checkInitialized();
		
		return new Aggregated(this, rset);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(String.format("%s: ", getClass().getSimpleName()));
		
		String aggrsStr = FStream.of(m_reducer.getValueAggregateAll())
								.map(ValueAggregate::toString)
								.join(",");
		builder.append("aggrs={").append(aggrsStr).append("}");
		
		return String.format(builder.toString());
	}

	public static AggregateIntermAtMapSide fromProto(AggregateIntermAtMapSideProto proto) {
		VARIntermediateReducer combiner
							= (VARIntermediateReducer)PBUtils.deserialize(proto.getReducer());
		
		return new AggregateIntermAtMapSide(combiner);
	}
	
	@Override
	public AggregateIntermAtMapSideProto toProto() {
		return AggregateIntermAtMapSideProto.newBuilder()
											.setReducer(((PBSerializable<?>)m_reducer).serialize())
											.build();
	}
	
	class Aggregated extends AbstractRecordSet implements ProgressReportable {
		private final AggregateIntermAtMapSide m_optor;
		private final RecordSet m_inputRSet;
		
		private RecordSet m_aggregateds = null;
		private long m_count =0;
		protected long m_elapsed;
		private boolean m_finalProgressReported = false;
		
		Aggregated(AggregateIntermAtMapSide optor, RecordSet input) {
			m_optor = optor;
			m_inputRSet = input;
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
			return String.format("%s: record_count=%,d, velo=%.0f/s", m_optor, m_count, velo);
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
			int index = 0;
			Record accumRecord = DefaultRecord.of(m_reducer.getRecordSchema());
			
			Record input = DefaultRecord.of(m_inputSchema);
			while ( rset.next(input) ) {
				m_reducer.reduce(accumRecord, input, index++);
				++m_count;
			}
			
			return RecordSet.of(accumRecord);
		}
	}
}
