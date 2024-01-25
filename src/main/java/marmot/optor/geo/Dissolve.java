package marmot.optor.geo;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.optor.CompositeRecordSetFunction;
import marmot.optor.Project;
import marmot.optor.TransformByGroup;
import marmot.optor.geo.reducer.AggrUnionGeom;
import marmot.optor.reducer.ValueAggregateReducer;
import marmot.proto.optor.DissolveProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Dissolve extends CompositeRecordSetFunction implements PBSerializable<DissolveProto> {
	private final MultiColumnKey m_groupCols;
	private final String m_geomColumn;
	private final FOption<Integer> m_workerCount;
	
	private Dissolve(MultiColumnKey groupCols, String geomCol, FOption<Integer> workerCount) {
		Preconditions.checkArgument(groupCols != null, "group column should not be null");
		
		m_groupCols = groupCols;
		m_geomColumn = geomCol;
		m_workerCount = workerCount;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema grpKeySchema = inputSchema.project(m_groupCols.getColumnNames());
		AggrUnionGeom union = new AggrUnionGeom(m_geomColumn);
		union.initializeWithInput(inputSchema);
		RecordSchema aggrSchema = union.getOutputValueSchema();
		
		return RecordSchema.concat(grpKeySchema, aggrSchema);
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		AggrUnionGeom unionGeom = new AggrUnionGeom(m_geomColumn);
		unionGeom.setOutputColumn(m_geomColumn);
		
		ValueAggregateReducer reducer = ValueAggregateReducer.from(unionGeom);
		TransformByGroup reduceByKey = TransformByGroup.builder()
														.keyColumns(m_groupCols.toString())
														.transform(reducer)
														.workerCount(m_workerCount)
														.build();
		
		Project project = new Project("the_geom,*-{the_geom}");

		return RecordSetOperatorChain.from(m_marmot, m_inputSchema).addAll(reduceByKey, project);
	}
	
	@Override
	public String toString() {
		return String.format("dissolve[group=%s,geom=%s%s]",
							m_groupCols, m_geomColumn!=null ? m_geomColumn : "?",
							m_workerCount.map(c -> ",nworkers=" + c).getOrElse(""));
	}

	public static Dissolve fromProto(DissolveProto proto) {
		return new Dissolve(MultiColumnKey.fromString(proto.getKeyColumns()),
							proto.getGeometryColumn(),
							PBUtils.getOptionField(proto, "group_worker_count"));
	}

	@Override
	public DissolveProto toProto() {
		DissolveProto.Builder builder = DissolveProto.newBuilder()
													.setKeyColumns(m_groupCols.toString())
													.setGeometryColumn(m_geomColumn);
		m_workerCount.ifPresent(cnt -> builder.setGroupWorkerCount(cnt));
		
		return builder.build();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private MultiColumnKey m_groupCols;
		private String m_geomCol;
		private FOption<Integer> m_workerCount = FOption.empty();
		
		public Dissolve build() {
			return new Dissolve(m_groupCols, m_geomCol, m_workerCount);
		}
		
		public Builder geometryColumn(String col) {
			m_geomCol = col;
			return this;
		}
		
		public Builder groupColumns(MultiColumnKey groupCols) {
			m_groupCols = groupCols;
			return this;
		}
		
		public Builder groupColumns(String colExpr) {
			m_groupCols = MultiColumnKey.fromString(colExpr);
			return this;
		}
		
		public Builder workerCount(FOption<Integer> workerCount) {
			m_workerCount = workerCount;
			return this;
		}
	}
}
