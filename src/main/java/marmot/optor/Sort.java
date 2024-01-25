package marmot.optor;

import java.util.Iterator;
import java.util.List;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordComparator;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.io.MultiColumnKey;
import marmot.mapreduce.MarmotMapOutputKeyColumns;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.SortProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.PBSerializable;
import utils.stream.FStream;


/**
 * {@code Sort}연산은 미리 지정된 컬럼 값을 기준으로 레코드 세트에 포함된 모든 레코드를
 * 정렬한 레코드 세트를 생성하는 작업을 수행한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Sort extends AbstractRecordSetFunction implements PBSerializable<SortProto> {
	private final MultiColumnKey m_sortKey;
	
	public static Sort by(String sortColSpecs) {
		return new Sort(MultiColumnKey.fromString(sortColSpecs));
	}
	
	public static Sort by(MultiColumnKey sortKey) {
		return new Sort(sortKey);
	}
	
	private Sort(MultiColumnKey sortKey) {
		m_sortKey = sortKey;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		List<String> badKeyCols = FStream.from(m_sortKey.getColumnNames())
										.filter(n -> !inputSchema.existsColumn(n))
										.toList();
		if ( badKeyCols.size() > 0 ) {
			throw new IllegalArgumentException("invalid sort key columns: op=" + this
												+ "bad-keys=" + badKeyCols);
		}
		
		setInitialized(marmot, inputSchema, inputSchema);
	}
	
	@Override
	public boolean isStreamable() {
		return false;
	}

	@Override
	public MapReduceJoint getMapReduceJoint(RecordSchema inputSchema) {
		Nop nop = Nop.get();
		nop.initialize(m_marmot, inputSchema);
		
		return MapReduceJoint.create()
					.setMapOutputKey(MarmotMapOutputKeyColumns.fromSortKey(m_sortKey))
					.setReducerCount(1)
					.addReducer(nop);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new Sorted(this, input);
	}
	
	@Override
	public String toString() {
		return String.format("sort[{%s}]", m_sortKey);
	}
	
	private static class Sorted extends AbstractRecordSet {
		private final Sort m_optor;
		private final RecordSet m_input;
		private Iterator<Record> m_iter = null;
		
		public Sorted(Sort sort, RecordSet input) {
			m_optor = sort;
			m_input = input;
		}
		
		@Override
		protected void closeInGuard() {
			m_input.closeQuietly();
		}

		@Override
		public RecordSchema getRecordSchema() {
			return m_optor.getRecordSchema();
		}

		@Override
		public boolean next(Record record) throws RecordSetException {
			checkNotClosed();
			
			// 레코드 세트의 첫번째 next() 메소드 호출인 경우는 먼저 정렬 작업을 수행한다.
			if ( m_iter == null ) {
				m_iter = m_input.fstream()
								.sort(new RecordComparator(m_optor.m_sortKey))
								.iterator();
			}
			
			if ( m_iter.hasNext() ) {
				record.setAll(m_iter.next().getAll());
				
				return true;
			}
			else {
				return false;
			}
		}
	}

	public static Sort fromProto(SortProto proto) {
		return new Sort(MultiColumnKey.fromString(proto.getSortColumns()));
	}

	@Override
	public SortProto toProto() {
		return SortProto.newBuilder()
						.setSortColumns(m_sortKey.toString())
						.build();
	}
}
