package marmot.optor.geo;

import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetType;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geowave.LoadGHdfsFile;
import marmot.optor.CompositeRecordSetLoader;
import marmot.optor.LoadCustomTextFile;
import marmot.optor.LoadMarmotFile;
import marmot.optor.geo.cluster.LoadSpatialClusterFile;
import marmot.optor.geo.filter.FilterSpatially;
import marmot.optor.geo.index.LoadSpatialIndexedFile;
import marmot.plan.PredicateOptions;
import marmot.proto.optor.QueryDataSetProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.CSV;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class QueryDataSet extends CompositeRecordSetLoader
							implements PBSerializable<QueryDataSetProto> {
	private final String m_dsId;
	private final QueryRange m_range;
	private PredicateOptions m_opts = PredicateOptions.DEFAULT;
	
	private QueryDataSet(String dsId, QueryRange range) {
		Utilities.checkNotNullArgument(dsId, "input dataset id");
		Utilities.checkNotNullArgument(range, "QueryRange");
		
		m_dsId = dsId;
		m_range = range;
	}
	
	public static QueryDataSet create(String dsId, Envelope bounds, PredicateOptions opts) {
		return new QueryDataSet(dsId, QueryRange.of(bounds).options(opts));
	}
	
	public static QueryDataSet create(String dsId, String keyDsId, PredicateOptions opts) {
		return new QueryDataSet(dsId, QueryRange.fromDataSet(keyDsId).options(opts));
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot) {
		DataSet ds = marmot.getDataSet(m_dsId);
		
		return ds.getRecordSchema();
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		DataSet ds = m_marmot.getDataSet(m_dsId);

		if ( !ds.hasGeometryColumn() ) {
			throw new RecordSetException("Dataset has not default Geometry column: "
										+ "dataset=" + m_dsId);
		}
		
		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot);
		
		FOption<SpatialIndexInfo> oinfo = ds.getSpatialIndexInfo();
		if ( !m_opts.negated().getOrElse(false) && oinfo.isPresent() ) {
			return createForIndexedRangeQuery(m_marmot, ds, oinfo.getUnchecked());
		}
		else {
			return createForNonIndexRangeQuery(m_marmot, ds);
		}
	}
	
	private RecordSetOperatorChain createForNonIndexRangeQuery(MarmotCore marmot, DataSet ds) {
		DataSetType type = ds.getType();
		Path path = new Path(ds.getHdfsPath());
		
		Envelope range = QueryDataSet.resolve(m_marmot, m_range);

		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot);
		if ( DataSetType.SPATIAL_CLUSTER.equals(type) ) {
			LoadSpatialClusterFile load = new LoadSpatialClusterFile(path);
			if ( !m_opts.negated().getOrElse(false) ) {
				// negation이 아니라면 검색 영역을 설정하고 바로 반환한다.
				return chain.add(load.setQueryRange(range));
			}
		}
		
		if ( DataSetType.FILE.equals(type) ) {
			chain.add(LoadMarmotFile.from(path));
		}
		else if ( DataSetType.TEXT.equals(type) ) {
			chain.add(new LoadCustomTextFile(path.toString()));
		}
		else if ( type == DataSetType.GWAVE ) {
			String layerName = CSV.parseCsv(path.toString(), '/').findLast().get();
			chain.add(LoadGHdfsFile.from(layerName));
		}
		else {
			throw new IllegalArgumentException("unsupported DataSetType: type=" + type);
		}

		FilterSpatially filter = new FilterSpatially(ds.getGeometryColumn(),
													SpatialRelation.INTERSECTS, range,
													PredicateOptions.DEFAULT);
		chain.add(filter);

		return chain;
	}
	
	private RecordSetOperatorChain createForIndexedRangeQuery(MarmotCore marmot, DataSet ds,
																SpatialIndexInfo idxInfo) {
		Envelope range = QueryDataSet.resolve(m_marmot, m_range);

		RecordSetOperatorChain chain = RecordSetOperatorChain.from(m_marmot);		
		LoadSpatialIndexedFile load = new LoadSpatialIndexedFile(ds.getId())
											.setQueryRange(range);
		return chain.add(load);
		
		// 대상 데이터세트의 좌표계가 EPSG:4326이 아닌 경우에는 좌표계 변환에 따른 오차에 의해
		// LoadSpatialIndexedFile의 결과에 검색 대상 영역에 포함되지 않는 데이터가 포함되기도 하기
		// 때문에 원래 좌표계로 다시 한번 filtering을 수행한다.
//		if ( !info.srid().equals(WGS84) ) {
//			UnaryEnvelopeIntersects filter = new UnaryEnvelopeIntersects(ds.getGeometryColumn(),
//																	range, PredicateOptions.DEFAULT);
//			builder.add(filter);
//		}
	}
	
	public static Envelope resolve(MarmotCore marmot, QueryRange range) {
		switch ( range.getRangeCase() ) {
			case BOUNDS:
				return range.getRangeBounds();
			case DATASET:
				String dsId = range.getRangeKeyDataSet();
				try ( RecordSet rset = marmot.getDataSet(dsId).read() ) {
					Record first = rset.findFirst();
					if ( first == null ) {
						throw new IllegalArgumentException("key dataset is empty: dsId=" + dsId);
					}
					
					for ( Object value: first.getAll() ) {
						if ( value instanceof Geometry ) {
							return ((Geometry)value).getEnvelopeInternal();
						}
					}
					
					throw new IllegalArgumentException("query key geometry is missing, dsId=" + dsId);
				}
			default:
				throw new IllegalArgumentException("invalid range case: " + range.getRangeCase());
		}
	}
	
	@Override
	public String toString() {
		return String.format("query: ds=%s range=%s", m_dsId, m_range);
	}

	public static QueryDataSet fromProto(QueryDataSetProto proto) {
		return new QueryDataSet(proto.getDsId(), QueryRange.fromProto(proto.getRange()));
	}

	@Override
	public QueryDataSetProto toProto() {
		return QueryDataSetProto.newBuilder()
								.setDsId(m_dsId)
								.setRange(m_range.toProto())
								.build();
	}
}
