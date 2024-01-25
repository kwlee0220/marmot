package marmot.dataset;

import java.util.List;

import utils.Utilities;
import utils.rx.ProgressReporter;
import utils.stream.FStream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedFile;
import marmot.rset.ConcatedRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.DataType;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.subjects.BehaviorSubject;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ThumbnailSelectionRecordSet extends ConcatedRecordSet
									implements ProgressReporter<Integer> {
	private final DataSetImpl m_source;
	private final RecordSchema m_schema;
	private final List<GlobalIndexEntry> m_entries;
	private final double m_ratio;
	
	private int m_idx = 0;
	private final BehaviorSubject<Integer> m_subject = BehaviorSubject.create();
	
	ThumbnailSelectionRecordSet(DataSetImpl source, long nsamples) {
		m_source = source;
		m_ratio = ((double)nsamples)/source.getRecordCount();
		
		List<GlobalIndexEntry> entries = ((SpatialIndexedFile)m_source.getSpatialIndexFile())
													.getGlobalIndex()
													.getIndexEntryAll();
		m_entries = FStream.from(entries)
							.filter(e -> Math.round(e.getOwnedRecordCount() * m_ratio) >= 1)
							.toList();
		
		m_schema = source.getRecordSchema().toBuilder()
						.addColumn("__quadKey", DataType.STRING)
						.build();
	}

	@Override
	public RecordSchema getRecordSchema() {
		return m_schema;
	}

	@Override
	public Observable<Integer> getProgressObservable() {
		return m_subject;
	}

	@Override
	protected RecordSet loadNext() {
		while ( m_idx < m_entries.size() ) {
			GlobalIndexEntry entry = m_entries.get(m_idx);
			
			String quadKey = entry.quadKey();
			int selectionSize = (int)Math.round(entry.getOwnedRecordCount() * m_ratio);
			
			++m_idx;
			int progress = Math.round(((float)m_idx * 100) / m_entries.size());
			m_subject.onNext(progress);
			
			if ( selectionSize > 0 ) {
				DataType geomType = m_source.getRecordSchema()
											.getColumn(m_source.getGeometryColumn())
											.type();
				switch ( geomType.getTypeCode() ) {
					case POLYGON:
					case MULTI_POLYGON:
						return selectByArea(quadKey, selectionSize);
					default:
						return selectByRandom(entry, selectionSize);
				}
			}
		}
		
		return null;
	}
	
	private RecordSet selectByArea(String quadKey, int nsample) {
		try ( RecordSet rset = m_source.readSpatialCluster(quadKey) ) {
			FStream<Record> selecteds = rset.fstream()
											.takeTopK(nsample, this::compare)
											.map(r -> attachInfo(r, quadKey));
			return RecordSet.from(m_schema, selecteds);
		}
	}
	
	private RecordSet selectByRandom(GlobalIndexEntry entry, int nsamples) {
		String quadKey = entry.quadKey();
		long total = entry.getOwnedRecordCount();
		
		List<Record> sampled = m_source.readSpatialCluster(quadKey)
										.fstream()
										.sample(total, m_ratio)
										.toList();
		List<Record> selecteds = Utilities.shuffle(sampled);
		return RecordSet.from(m_schema,
							FStream.from(selecteds).map(r -> attachInfo(r, quadKey)));
	}
	
	private Record attachInfo(Record input, String quadKey) {
		Record output = DefaultRecord.of(m_schema);
		output.set(input);
		output.set("__quadKey", quadKey);
		
		return output;
	}
	
	private int compare(Record rec1, Record rec2) {
		double area1 = rec1.getGeometry("the_geom").getArea();
		double area2 = rec2.getGeometry("the_geom").getArea();
		
		return Double.compare(area2, area1);
	}
}
