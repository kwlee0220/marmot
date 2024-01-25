package marmot.optor.geo.advanced;

import java.util.List;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.support.DefaultRecord;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class LocalSpatialIndicator extends NestedLoopSpatialJoin<LocalSpatialIndicator> {
	protected final String m_valueColumn;
	protected final double m_radius;
	protected final LISAWeight m_wtype;
	
	private int m_valueColIdx;
	
	protected abstract void calculate(Record input, double xi, List<Neighbor> neighbors,
										Record output);
	
	public LocalSpatialIndicator(String inputGeomCol, String paramDsId,
								String valueColumn, double radius, LISAWeight wtype) {
		super(inputGeomCol, paramDsId, SpatialJoinOptions.WITHIN_DISTANCE(radius));
		
		Preconditions.checkArgument(valueColumn != null, "value column name is null");
		Preconditions.checkArgument(radius > 0, "invalid radius: radius=" + radius);
		Preconditions.checkArgument(wtype != null, "LISAWeight is null");
		
		m_valueColumn = valueColumn;
		m_radius = radius;
		m_wtype = wtype;
	}
	
	public LocalSpatialIndicator(String inputGeomCol, String paramDsId,
								String valueColumn, double radius) {
		this(inputGeomCol, paramDsId, valueColumn, radius, LISAWeight.FIXED_DISTANCE_BAND);
	}
	
	public LISAWeight getLISAWeight() {
		return m_wtype;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_valueColIdx = inputSchema.findColumn(m_valueColumn)
							.map(Column::ordinal)
							.getOrThrow(() -> new IllegalArgumentException("invalid value column: name=" + m_valueColumn));
		
		super.initialize(marmot, inputSchema);
	}
	
	protected int getValueColumnIndex() {
		return m_valueColIdx;
	}
	
	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		double xi = match.getOuterRecord().getDouble(m_valueColIdx);
		Geometry outerGeom = getOuterGeometry(match.getOuterRecord()); 
		List<Neighbor> neighbors = match.getInnerRecords()
								.map(innerRec -> {
									Neighbor neighbor = new Neighbor();
									
									Geometry innerGeom = getInnerGeometry(innerRec);
									neighbor.m_record = innerRec;
									neighbor.m_distance = outerGeom.distance(innerGeom);
									neighbor.m_wij = m_wtype.weigh(neighbor.m_distance, m_radius);
									neighbor.m_xj = innerRec.getDouble(m_valueColIdx);
									
									return neighbor;
								})
								.toList();
		
		Record output = DefaultRecord.of(getRecordSchema());
		calculate(match.getOuterRecord(), xi, neighbors, output);
		
		return RecordSet.of(output);
	}
}