package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.support.Filter;
import marmot.plan.PredicateOptions;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialFilter<T extends SpatialFilter<T>> extends Filter<T> {
	private final String m_geomCol;
	private int m_geomColIdx = -1;

	abstract protected void initialize(MarmotCore marmot, GeometryDataType inGeomType);
	abstract protected boolean test(Geometry geom);
	
	protected SpatialFilter(String geomCol, PredicateOptions opts) {
		super(opts);
		Utilities.checkNotNullArgument(geomCol, "geometry column is null");
		
		m_geomCol = geomCol;
		setLogger(LoggerFactory.getLogger(SpatialFilter.class));
	}
	
	public String getInputGeometryColumn() {
		return m_geomCol;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column col = inputSchema.getColumn(m_geomCol);
		initialize(marmot, (GeometryDataType)col.type());
		
		m_geomColIdx = col.ordinal();
		
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public boolean test(Record input) {
		try {
			Geometry geom = input.getGeometry(m_geomColIdx);
			return test(geom);
		}
		catch ( Exception ignored ) {
			getLogger().warn("" + ignored);
			
			return false;
		}
	}
	
	protected final Geometry loadKeyGeometry(MarmotCore marmot, String keyDsId) {
		try ( RecordSet rset = marmot.getDataSet(keyDsId).read() ) {
			Record first = rset.findFirst();
			if ( first == null ) {
				throw new IllegalArgumentException("key dataset is empty: op=" + this);
			}
			for ( Object value: first.getAll() ) {
				if ( value instanceof Geometry ) {
					return (Geometry)value;
				}
			}
			throw new IllegalArgumentException("query key geometry is missing, opt=" + this);
		}
	}
}
