package marmot.optor;

import java.util.Iterator;
import java.util.List;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.ClusterChroniclesProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.Interval;
import utils.LocalDateTimes;
import utils.UnitUtils;
import utils.func.Lazy;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterChronicles extends AbstractRecordSetFunction
								implements PBSerializable<ClusterChroniclesProto> {
	private final long m_threshold;
	private final String m_colName;
	private final String m_outColName;
	
	private int m_colIdx;
		
	public ClusterChronicles(String colName, String outColName, long threshold) {
		m_colName = colName;
		m_outColName = outColName;
		m_threshold = threshold;
	}
	
	public ClusterChronicles(String colName, String outColName, String threshold) {
		m_colName = colName;
		m_outColName = outColName;
		m_threshold = UnitUtils.parseDurationMillis(threshold);
	}
	
	public long getDuration() {
		return m_threshold;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column col = inputSchema.findColumn(m_colName)
						.getOrThrow(() -> {
							String details = String.format("%s: Chronicle column not found: name=%s",
															getClass(), m_colName);
							throw new IllegalArgumentException(details);
						});
		if ( !col.type().equals(DataType.DATETIME) ) {
			String details = String.format("%s: Chronicle column is not DateTime: name=%s",
											getClass(), m_colName);
			throw new IllegalArgumentException(details);
		}
		m_colIdx = col.ordinal();
		
		RecordSchema outSchema = RecordSchema.builder()
											.addColumn(m_outColName, DataType.INTERVAL)
											.build();
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return new Clustered(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("%s(threshold=%s)", getClass().getSimpleName(), m_threshold);
	}

	public static ClusterChronicles fromProto(ClusterChroniclesProto proto) {
		return new ClusterChronicles(proto.getInputColumn(), proto.getOutputColumn(),
									proto.getThreshold());
	}

	@Override
	public ClusterChroniclesProto toProto() {
		return ClusterChroniclesProto.newBuilder()
										.setInputColumn(m_colName)
										.setOutputColumn(m_outColName)
										.setThreshold("" + m_threshold)
										.build();
	}
	
	private List<Interval> cluster(RecordSet input) {
		List<Long> tsList = input.fstream()
								.map(r -> r.getDateTime(m_colIdx))
								.map(LocalDateTimes::toUtcMillis) 
								.toList();
		return Interval.cluster(tsList, m_threshold);
	}
	
	private class Clustered extends SingleInputRecordSet<ClusterChronicles> {
		private Lazy<Iterator<Interval>> m_iter;

		Clustered(ClusterChronicles func, RecordSet input) {
			super(func, input);
			
			m_iter = Lazy.of(() -> func.cluster(input).iterator());
		}

		@Override
		public boolean next(Record output) {
			checkNotClosed();
			
			if ( !m_iter.get().hasNext() ) {
				return false;
			}
			
			Interval intvl = m_iter.get().next();
			output.set(0, intvl);
			
			return true;
		}
		
		@Override
		public String toString() {
			return m_optor.toString();
		}
	}
}