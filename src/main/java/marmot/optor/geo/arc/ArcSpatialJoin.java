package marmot.optor.geo.arc;

import java.util.List;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.optor.geo.join.NestedLoopMatch;
import marmot.optor.geo.join.NestedLoopSpatialJoin;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.ArcSpatialJoinProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.func.FOption;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcSpatialJoin extends NestedLoopSpatialJoin<ArcSpatialJoin>
								implements PBSerializable<ArcSpatialJoinProto> {
	private static final String COL_JOIN_COUNT = "Join_Count";
	private final boolean m_oneToMany;
	private final boolean m_includeParamCols;
	private int m_paramColIdx = -1;
	private int m_paramGeomColIdx = -1;
	
	public ArcSpatialJoin(String inputGeomCol, String paramDataSet, boolean oneToMany,
							boolean includeParamCols, SpatialJoinOptions opts) {
		super(inputGeomCol, paramDataSet, opts);

		m_oneToMany = oneToMany;
		m_includeParamCols = includeParamCols;
	}

	@Override
	protected RecordSchema initialize(MarmotCore marmot, RecordSchema inputSchema,
									RecordSchema paramSchema) {
		Column geomCol = getOuterGeomColumn();

		RecordSchema.Builder builder = RecordSchema.builder()
											.addColumn(getOuterGeomColumn())
											.addColumn(COL_JOIN_COUNT, DataType.INT);
		inputSchema.streamColumns()
					.filter(col -> !col.name().equals(geomCol.name()))
					.collect(builder, (b,c) -> b.addColumn(c));
		
		m_paramColIdx = builder.size();
		if ( m_includeParamCols ) {
			Column paramGeomCol = getInnerGeomColumn();
			paramSchema.streamColumns()
						.filter(col -> !col.name().equals(paramGeomCol.name()))
						.forEach(col -> builder.addColumn(col.name() + "_1", col.type()));
		}
		
		m_paramGeomColIdx = getInnerGeomColumn().ordinal();
		
		return builder.build();
	}

	@Override
	protected RecordSet doInnerLoop(NestedLoopMatch match) {
		List<Object> outerValues = Lists.newArrayList(match.getOuterRecord().getAll());
		outerValues.add(1, 0);
		Record output = DefaultRecord.of(getRecordSchema());
		output.setAll(outerValues);
		
		return handleMatches(output, match.getInnerRecords());
	}
	
	@Override
	public String toString() {
		return String.format("arc_join: %s<->%s, inc_param=%s",
								getOuterGeomColumn(), getParamDataSetId(),
								m_oneToMany);
	}

	public static ArcSpatialJoin fromProto(ArcSpatialJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.DEFAULT;
		switch ( proto.getOptionalJoinExprCase() ) {
			case JOIN_EXPR:
				opts = opts.joinExpr(proto.getJoinExpr());
				break;
			case OPTIONALJOINEXPR_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return new ArcSpatialJoin(proto.getGeomColumn(), proto.getParamDataset(),
									proto.getOneToMany(), proto.getIncludeParamCols(),
									opts);
	}

	@Override
	public ArcSpatialJoinProto toProto() {
		ArcSpatialJoinProto.Builder builder = ArcSpatialJoinProto.newBuilder()
													.setGeomColumn(getOuterGeomColumnName())
													.setParamDataset(getParamDataSetId())
													.setOneToMany(m_oneToMany)
													.setIncludeParamCols(m_includeParamCols);
		getSpatialJoinOptions().joinExpr()
								.ifPresent(builder::setJoinExpr);
		return builder.build();
	}
	
	private RecordSet handleMatches(Record output, FStream<Record> inners) {
		int count = 0;
		
		if ( m_oneToMany ) {
			List<Record> joineds = Lists.newArrayList();
			
			FOption<Record> inner;
			while ( (inner = inners.next()).isPresent() ) {
				if ( m_includeParamCols ) {
					joineds.add(fillParamDataColumns(output.duplicate(), inner.getUnchecked()));
				}
				++count;
			}
			for ( Record joined: joineds ) {
				joined.set(1, 1);
			}
			
			return RecordSet.from(joineds);
		}
		else {
			FOption<Record> inner;
			while ( (inner = inners.next()).isPresent() ) {
				if ( count == 0 && m_includeParamCols ) {
					fillParamDataColumns(output, inner.getUnchecked());
				}
				++count;
			}
			output.set(1, count);
			
			return RecordSet.of(output);
		}
	}
	
	private Record fillParamDataColumns(Record output, Record inner) {
		List<Object> paramValues = Lists.newArrayList(inner.getAll());
		paramValues.remove(m_paramGeomColIdx);
		output.setAll(m_paramColIdx, paramValues);
		
		return output;
	}
}
