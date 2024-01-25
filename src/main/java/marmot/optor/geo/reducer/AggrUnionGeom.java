package marmot.optor.geo.reducer;

import java.util.List;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.AggregateFunction;
import marmot.optor.AggregateType;
import marmot.optor.reducer.ValueAggregate;
import marmot.optor.support.SafeUnion;
import marmot.support.GeoUtils;
import marmot.type.DataType;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggrUnionGeom implements ValueAggregate {
	private static final int BATCH_SIZE = 128;
	
	private final String m_colName;
	private String m_outColName;
	
	private Column m_inputCol = null;
	private DataType m_intermType;
	private int m_intermColIdx = -1;

	public AggrUnionGeom(String colName) {
		m_colName = colName;
		m_outColName = AggregateFunction.UNION_GEOM(colName).m_resultColumn;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.UNION_GEOM;
	}

	@Override
	public String getAggregateColumn() {
		return m_colName;
	}

	@Override
	public String getOutputColumn() {
		return m_outColName;
	}

	@Override
	public void setOutputColumn(String colName) {
		Utilities.checkNotNullArgument(colName, "colName is null");
		
		m_outColName = colName;
	}

	@Override
	public void initializeWithInput(RecordSchema inputSchema) {
		m_inputCol = inputSchema.getColumn(m_colName);
		m_intermType = computeIntermediateType(m_inputCol.type());
	}

	@Override
	public void setIntermediateSchema(RecordSchema intermediateSchema) {
		Column intermCol = intermediateSchema.getColumn(m_outColName);
		m_intermType = intermCol.type();
		m_intermColIdx = intermCol.ordinal();
	}

	@Override
	public void initializeWithIntermediate(RecordSchema intermediateSchema) {
		Column intermCol = intermediateSchema.getColumn(m_outColName);
		m_intermType = intermCol.type();
		m_intermColIdx = intermCol.ordinal();
	}

	@Override
	public RecordSchema getIntermediateValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, m_intermType)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, m_intermType)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Object value = input.get(m_inputCol.ordinal());

		Object interm;
		switch ( Geometries.get((Geometry)value) ) {
			case MULTIPOLYGON:
			case MULTILINESTRING:
			case MULTIPOINT:
			case GEOMETRYCOLLECTION:
				interm = value;
				break;
			case POLYGON:
				interm = GeoUtils.toMultiPolygon((Polygon)value);
				break;
			case LINESTRING:
				interm = GeoUtils.toMultiLineString((LineString)value);
				break;
			case POINT:
				interm = GeoUtils.toMultiPoint((Point)value);
				break;
			default:
				throw new AssertionError("unexpected value: " + Geometries.get((Geometry)value));
		}
		
		intermediate.set(m_intermColIdx, interm);
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		GeometryCollection accumData = (GeometryCollection)accum.getGeometry(m_intermColIdx);
		Geometry data = intermediate.getGeometry(m_intermColIdx);
		
		List<Geometry> comps = GeoUtils.streamSubComponents(accumData).toList();
		comps.add(data);
		if ( comps.size() >= BATCH_SIZE ) {
			comps = Lists.newArrayList(aggregate(comps));
		}
		
		accum.set(m_intermColIdx, GeoUtils.toGeometryCollection(comps));
	}

	@Override
	public void toFinal(Record accum, Record output) {
		GeometryCollection accumData = (GeometryCollection)accum.getGeometry(m_intermColIdx);
		
		List<Geometry> comps = GeoUtils.streamSubComponents(accumData).toList();
		Geometry result = aggregate(comps);
		
		Geometry outGeom;
		switch ( m_intermType.getTypeCode() ) {
			case MULTI_POLYGON:
				List<Polygon> polys = GeoUtils.flatten(result, Polygon.class);
				outGeom = GeoUtils.toMultiPolygon(polys);
				break;
			case MULTI_POINT:
				List<Point> pts = GeoUtils.flatten(result, Point.class);
				outGeom = GeoUtils.toMultiPoint(pts);
				break;
			case MULTI_LINESTRING:
				List<LineString> lines = GeoUtils.flatten(result, LineString.class);
				outGeom = GeoUtils.toMultiLineString(lines);
				break;
			case GEOM_COLLECTION:
				outGeom = GeoUtils.toGeometryCollection(comps);
				break;
			default:
				throw new AssertionError();
		}
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), outGeom);
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s,out=%s)", AggregateType.UNION_GEOM, m_colName, m_outColName);
	}
	
	private Geometry aggregate(List<Geometry> comps) {
		if ( comps.size() == 1 ) {
			return comps.get(0);
		}
		
		Geometries resultType = getUnionType(Geometries.get(comps.get(0)));
		SafeUnion union = new SafeUnion(resultType);
		
		return union.apply(comps);
	}
	
	private Geometries getUnionType(Geometries srcType) {
		switch (srcType ) {
			case POLYGON:
			case MULTIPOLYGON:
				return Geometries.MULTIPOLYGON;
			case POINT:
			case MULTIPOINT:
				return Geometries.MULTIPOINT;
			case LINESTRING:
			case MULTILINESTRING:
				return Geometries.MULTILINESTRING;
			default:
				return Geometries.GEOMETRYCOLLECTION;
		}
	}
	
	private DataType computeIntermediateType(DataType inputDataType) {
		switch ( inputDataType.getTypeCode() ) {
			case MULTI_POLYGON:
			case POLYGON:
				return DataType.MULTI_POLYGON;
			case LINESTRING: case MULTI_LINESTRING:
				return DataType.MULTI_LINESTRING;
			case POINT: case MULTI_POINT:
				return DataType.MULTI_POINT;
			case GEOM_COLLECTION:
				return DataType.GEOM_COLLECTION;
			default:
				throw new AssertionError("unexpected DataType: " + inputDataType);
		}
	}
}