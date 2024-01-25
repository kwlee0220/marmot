package marmot.optor.geo.reducer;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.geo.GeoClientUtils;
import marmot.optor.AggregateType;
import marmot.optor.reducer.ValueAggregate;
import marmot.type.DataType;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AggrEnvelope implements ValueAggregate {
	private static final String DEFAULT_OUT_COLUMN = "envelope";
	
	private final String m_colName;
	private String m_outColName = DEFAULT_OUT_COLUMN;
	
	private Column m_inputCol = null;
	private int m_intermColIdx = -1;

	public AggrEnvelope(String colName) {
		m_colName = colName;
	}

	@Override
	public AggregateType getAggregateType() {
		return AggregateType.ENVELOPE;
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
	}

	@Override
	public void setIntermediateSchema(RecordSchema intermediateSchema) {
		m_intermColIdx = intermediateSchema.getColumn(m_outColName).ordinal();
	}

	@Override
	public void initializeWithIntermediate(RecordSchema intermediateSchema) {
		m_intermColIdx = intermediateSchema.getColumn(m_outColName).ordinal();
	}

	@Override
	public RecordSchema getIntermediateValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.ENVELOPE)
							.build();
	}

	@Override
	public RecordSchema getOutputValueSchema() {
		return RecordSchema.builder()
							.addColumn(m_outColName, DataType.POLYGON)
							.build();
	}

	@Override
	public void toIntermediate(Record input, Record intermediate) {
		Geometry geom = input.getGeometry(m_inputCol.ordinal());
		intermediate.set(m_intermColIdx, geom.getEnvelopeInternal());
	}

	@Override
	public void aggregate(Record accum, Record intermediate) {
		Envelope accumData = (Envelope)accum.get(m_intermColIdx);
		Envelope data = (Envelope)intermediate.get(m_intermColIdx);
		accumData.expandToInclude(data);
	}

	@Override
	public void toFinal(Record accum, Record output) {
		Envelope accumData = (Envelope)accum.get(m_intermColIdx);
		Polygon poly = GeoClientUtils.toPolygon(accumData);
		
		Column col = output.getRecordSchema().getColumn(m_outColName);
		output.set(col.ordinal(), poly);
	}
}