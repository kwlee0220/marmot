package marmot.dataset;

import java.io.IOException;
import java.util.Set;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import com.google.common.collect.Sets;

import utils.Tuple;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.geo.query.RangeQueryEstimate;
import marmot.io.MarmotFileNotFoundException;
import marmot.io.MarmotSequenceFile;
import marmot.rset.AbstractRecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ThumbnailRecordSet extends AbstractRecordSet {
	private final RecordSet m_input;
	private final double m_sampleRatio;
	private final RecordSchema m_schema;
	private final Set<String> m_visiteds = Sets.newHashSet();
	
	private final RangeQueryEstimate m_est;
	private final int m_geomIdx;
	private final int m_qkeyIdx;
	private final PreparedGeometry m_prange;
	private String m_lastQKey = "X";
	private int m_threshold;
	private int m_remains = 0;

	ThumbnailRecordSet(DataSetImpl ds, Envelope range, int sampleCount) throws IOException {
		m_est = ds.estimateRangeQuery(range);
		m_sampleRatio = (double)sampleCount / m_est.getMatchCount();
		
		try {
			m_input = MarmotSequenceFile.of(ds.getThumbnailPath()).read();
			
			m_schema = m_input.getRecordSchema();
			m_geomIdx = m_schema.getColumn(ds.getGeometryColumn()).ordinal();
			m_qkeyIdx = m_schema.getColumn("__quadKey").ordinal();
			m_prange = PreparedGeometryFactory.prepare(GeoClientUtils.toPolygon(range));
			
		}
		catch ( MarmotFileNotFoundException e ) {
			throw new ThumbnailNotFoundException(ds.getId());
		}
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	protected void closeInGuard() throws Exception {
		m_input.close();
	}
	
	@Override
	public boolean next(Record output) {
		if ( !m_input.next(output) ) {
			return false;
		}
		
		String qkey = output.getString(m_qkeyIdx);
		if ( !m_lastQKey.equals(qkey) ) {
			// 현재 cluster에서 더 sampling 해야할 레코드가 남아있는데 (m_remains)
			// thumbnail에 남은 레코드가 없는 경우는 thumbnail에 충분한 데이터가 없다는
			// 것을 의미하므로 'InsufficientThumbnailException' 예외를 발생시킨다.
			//
			if ( m_remains > m_threshold ) {
				String msg = String.format("quad_key=%s, shortage=%d", qkey, m_remains);
				throw new InsufficientThumbnailException(msg);
			}
			
			if ( !setup(output) ) {
				return false;
			}
		}
		else if ( m_remains == 0 ) {
			if ( m_lastQKey != null ) {
//System.out.println("\tskip: " + output.getString(m_qkeyIdx));
				if ( !skipRemaing(m_lastQKey, output) ) {
					return false;
				}
			}

			if ( !setup(output) ) {
				return false;
			}
		}
		
		--m_remains;
//System.out.printf("\tqkey=%s, remains=%d%n", m_lastQKey, m_remains);
		return true;
	}
	
	private boolean setup(Record output) {
		while ( true ) {
			m_lastQKey = output.getString(m_qkeyIdx);
			if ( !m_visiteds.add(m_lastQKey) ) {
				throw new IllegalStateException("already visited: quad-key=" + m_lastQKey);
			}
			
			int matchCount = m_est.getClusterEstimate(m_lastQKey)
									.map(est -> est.getMatchCount())
									.getOrElse(0);
			m_remains = (int)Math.round(matchCount * m_sampleRatio);
			m_threshold = Math.round(m_remains * 0.05f);
//System.out.printf("setup: qkey=%s, samples=%d, total=%d%n", m_lastQKey, m_remains, m_est.getMatchingRecordCount(m_lastQKey));

			if ( m_remains > 0 ) {
//System.out.println();
				Tuple<Boolean,Boolean> ret = findNextMatch(m_lastQKey, output);
				if ( ret._2 ) {
					return true;
				}
				else if ( !ret._1 ) {
					return false;
				}
			}
			else {
//System.out.println(" -> skip rest");
				if ( !skipRemaing(m_lastQKey, output) ) {
					return false;
				}
			}
//			else if ( !skipRemaing(m_lastQKey, output) ) {
//				return false;
//			}
		}
	}
	
	private Tuple<Boolean,Boolean> findNextMatch(String quadKey, Record inout) {
		while ( true ) {
			Geometry geom = inout.getGeometry(m_geomIdx);
			if ( m_prange.intersects(geom) ) {
				return Tuple.of(true,true);
			}
			
			if ( !m_input.next(inout) ) {
				return Tuple.of(false, false);
			}
			if ( !quadKey.equals(inout.getString(m_qkeyIdx)) ) {
				return Tuple.of(true, false);
			}
		}
	}
	
	private boolean skipRemaing(String quadKey, Record output) {
		while ( m_input.next(output) ) {
			if ( !quadKey.equals(output.getString(m_qkeyIdx)) ) {
				return true;
			}
		}
		
		return false;
	}
}
