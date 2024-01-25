package marmot.optor.geo.filter;

import org.locationtech.jts.geom.Geometry;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.Filter;
import marmot.plan.PredicateOptions;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class BinarySpatialFilter<T extends BinarySpatialFilter<T>> extends Filter<T> {
	private String m_leftGeomCol;
	private String m_rightGeomCol;
	private int m_leftGeomColIdx = -1;
	private int m_rightGeomColIdx = -1;

	abstract protected void initialize(GeometryDataType leftGeomType,
										GeometryDataType rightGeomType);
	abstract protected boolean test(Geometry leftGeom, Geometry rightGeom);
	
	protected BinarySpatialFilter(String leftGeomCol, String rightGeomCol, PredicateOptions opts) {
		super(opts);
		
		m_leftGeomCol = leftGeomCol;
		m_rightGeomCol = rightGeomCol;
	}
	
	public String getLeftGeometryColumn() {
		return m_leftGeomCol;
	}
	
	public String getRightGeometryColumn() {
		return m_rightGeomCol;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		Column leftGeomCol = inputSchema.getColumn(m_leftGeomCol);
		Column rightGeomCol = inputSchema.getColumn(m_rightGeomCol);
		
		m_leftGeomColIdx = leftGeomCol.ordinal();
		m_rightGeomColIdx = rightGeomCol.ordinal();
		
		if ( m_leftGeomColIdx == m_rightGeomColIdx ) {
			throw new IllegalArgumentException("left and right geometries are same");
		}
		
		initialize((GeometryDataType)leftGeomCol.type(),
					(GeometryDataType)rightGeomCol.type());
		
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public boolean test(Record record) {
		Geometry leftGeom = record.getGeometry(m_leftGeomColIdx);
		Geometry rightGeom = record.getGeometry(m_rightGeomColIdx);
		return test(leftGeom, rightGeom);
	}
}
