package marmot.optor.geo.arc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.geotools.geometry.jts.Geometries;
import org.locationtech.jts.geom.Geometry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.module.geo.arc.ArcGisUtils;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.SafeDifference;
import marmot.optor.support.SafeIntersection;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.ArcUnionPhase1Proto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import marmot.type.TypeCode;

import utils.Tuple;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcUnionPhase1 extends NestedLoopSpatialJoin<ArcUnionPhase1>
									implements PBSerializable<ArcUnionPhase1Proto> {
	private final String m_leftKeyCols;
	private final String m_rightKeyCols;
	private ColumnSelector m_selector;
	private SafeIntersection m_intersection;
	private SafeDifference m_difference;
	private Map<String,Record> m_binding = Maps.newHashMap();
	
	public ArcUnionPhase1(String leftGeomCol, String rightDsId, String leftKeyCols,
							String rightKeyCols, SpatialJoinOptions opts) {
		super(leftGeomCol, rightDsId, opts);
		
		m_leftKeyCols = leftKeyCols;
		m_rightKeyCols = rightKeyCols;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema leftSchema,
										RecordSchema rightSchema) {
		Column geomCol = getOuterGeomColumn();
		DataType geomType = geomCol.type();
		if ( geomType.getTypeCode() != TypeCode.POLYGON ) {
			throw new IllegalArgumentException("input geometry is not POLYGON, type=" + geomType);
		}
		
		Column innerGeomCol = getInnerGeomColumn();
		DataType innerGeomType = innerGeomCol.type();
		switch ( innerGeomType.getTypeCode() ) {
			case POLYGON: case MULTI_POLYGON:
				break;
			default:
				throw new IllegalArgumentException("parameter geometry is not POLYGON, type="
													+ innerGeomType);
		}

		m_intersection = new SafeIntersection(Geometries.MULTIPOLYGON)
								.setReduceFactor(2);
		m_difference = new SafeDifference(Geometries.MULTIPOLYGON)
								.setReduceFactor(2);
		
		String leftCols = leftSchema.streamColumns()
									.map(Column::name)
									.filter(n -> !n.equals(geomCol.name()))
									.join(',');
		String rightCols = rightSchema.streamColumns()
									.map(Column::name)
									.filter(n -> !n.equals(geomCol.name()))
									.join(',');
		String outCols = String.format("left.the_geom,%s",
										ArcGisUtils.combineColumns(leftCols, rightCols));
		try {
			m_selector = JoinUtils.createJoinColumnSelector(leftSchema, rightSchema, outCols);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
		
		RecordSchema outSchema = m_selector.getRecordSchema();
		super.setInitialized(marmot, leftSchema, outSchema);
		return outSchema;
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		m_binding.put("left", match.getOuterRecord());
		
		List<Record> matcheds = union(match);
		return RecordSet.from(matcheds);
	}
	
	@Override
	public String toString() {
		return String.format("arc_union_phase1[%s<->%s]", getOuterGeomColumnName(),
								getParamDataSetId());
	}

	public static ArcUnionPhase1 fromProto(ArcUnionPhase1Proto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new ArcUnionPhase1(proto.getLeftGeomColumn(), proto.getRightDataset(),
								proto.getLeftKeyColumns(), proto.getRightKeyColumns(), opts);
	}

	@Override
	public ArcUnionPhase1Proto toProto() {
		return ArcUnionPhase1Proto.newBuilder()
									.setLeftGeomColumn(getOuterGeomColumnName())
									.setRightDataset(getParamDataSetId())
									.setLeftKeyColumns(m_leftKeyCols)
									.setRightKeyColumns(m_rightKeyCols)
									.setOptions(m_options.toProto())
									.build();
	}
	
	private List<Record> union(NestedLoopMatch match) {
		Record outer = match.getOuterRecord();
		Geometry geom = getOuterGeometry(outer);
		
		FStream<Record> inners = match.getInnerRecords();
		FOption<Record> first = inners.next();
		if ( first.isAbsent() ) {
			m_binding.remove("right");
			return Collections.singletonList(m_selector.select(m_binding));
		}
		
		List<Record> outputList = Lists.newArrayList();
		m_binding.put("right", first.get());
		outputList.add(m_selector.select(m_binding));
		
		List<Record> matcheds = Lists.newArrayList();
		for ( Record inner: inners ) {
			Tuple<Geometry,Record> out = toOutputRecord(geom, inner);
			matcheds.add(out._2);
			geom = out._1;
		}
		if ( !geom.isEmpty() ) {
			m_binding.put("right", DefaultRecord.of(match.getInnerRecordSchema()));
			Record output = m_selector.select(m_binding);
			output.set(0, geom);
			matcheds.add(output);
		}
		
		return matcheds;
	}
	
	private Tuple<Geometry,Record> toOutputRecord(Geometry outerGeom, Record inner) {
		m_binding.put("right", inner);
		Record output = m_selector.select(m_binding);
		
		Geometry innerGeom = getInnerGeometry(inner);
		Geometry diff = m_difference.apply(outerGeom, innerGeom);
		output.set(0, m_intersection.apply(outerGeom, innerGeom));
		
		return Tuple.of(diff, output);
	}
}
