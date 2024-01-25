package marmot.module.geo;

import static marmot.optor.AggregateFunction.COUNT;
import static marmot.optor.JoinOptions.INNER_JOIN;
import static marmot.optor.StoreDataSetOptions.FORCE;
import static utils.Utilities.checkNotNullArgument;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;

import marmot.BindDataSetOptions;
import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.analysis.module.geo.DBSCANParameters;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.io.MarmotFileWriteOptions;
import marmot.module.MarmotModule;
import marmot.plan.Group;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DBSCAN implements MarmotModule {
	public static final String NAME = "dbscan";

	private MarmotCore m_marmot;
	private DBSCANParameters m_params;
	private RecordSchema m_inputSchema;
	private RecordSchema m_outputSchema;
	
	private String m_idCol;
	private String m_clusterCol;
	private String m_typeCol;

	@Override
	public List<String> getParameterNameAll() {
		return DBSCANParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot, Map<String, String> paramsMap) {
		DBSCANParameters params = DBSCANParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.inputDataSet());
			
		return input.getRecordSchema().toBuilder()
					.addColumn(params.clusterColumn(), DataType.STRING)
					.addColumn(params.typeColumn(), DataType.STRING)
					.build();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = DBSCANParameters.fromMap(paramsMap);
		
		checkNotNullArgument(m_params.inputDataSet(), "parameter is missing: " + DBSCANParameters.INPUT_DATASET);
		checkNotNullArgument(m_params.outputDataSet(), "parameter is missing: " + DBSCANParameters.OUTPUT_DATASET);
		checkNotNullArgument(m_params.idColumn(), "parameter is missing: " + DBSCANParameters.ID_COLUMN);
		checkNotNullArgument(m_params.clusterColumn(), "parameter is missing: " + DBSCANParameters.CLUSTER_COLUMN);
		checkNotNullArgument(m_params.typeColumn(), "parameter is missing: " + DBSCANParameters.TYPE_COLUMN);
		
		DataSet input = marmot.getDataSet(m_params.inputDataSet());
		m_inputSchema = input.getRecordSchema();
		if ( m_inputSchema.findColumn(m_params.idColumn()).isAbsent() ) {
			throw new IllegalArgumentException("missing id column: " + m_params.idColumn());
		}
		m_outputSchema = m_inputSchema.toBuilder()
										.addColumn(m_params.clusterColumn(), DataType.STRING)
										.addColumn(m_params.typeColumn(), DataType.STRING)
										.build();
	}
	
	private static final String SUFFIX_BASE = "/base";
	private static final String SUFFIX_JOIN = "/join";
	private static final String SUFFIX_OUT_CNT = "/out_counts";
	private static final String SUFFIX_IN_CNT = "/in_counts";
	private static final String SUFFIX_STEP = "/step";

	@Override
	public void run() {
		m_idCol = m_params.idColumn();
		m_clusterCol = m_params.clusterColumn();
		m_typeCol = m_params.typeColumn();
		
		DataSet input = m_marmot.getDataSet(m_params.inputDataSet());
		GeometryColumnInfo gcInfo = input.getGeometryColumnInfo();
		
		String tempDir = "/tmp/" + UUID.randomUUID().toString();
//		String tempDir = "/tmp/dbscan";
		String baseDsId = tempDir + SUFFIX_BASE;
		String prjExpr = String.format("%s,%s", gcInfo.name(), m_idCol);

		Plan plan;
		plan = Plan.builder()
					.load(m_params.inputDataSet())
					.project(prjExpr)
					.store(baseDsId, FORCE(gcInfo))
					.build();
		m_marmot.execute(plan);
		m_marmot.getDataSet(baseDsId).cluster(ClusterSpatiallyOptions.FORCE);

		String joinDsId = tempDir + SUFFIX_JOIN;
		String outputCols = String.format("left.*,right.{%s as %s}", m_idCol, m_clusterCol);
		plan = Plan.builder()
					.load(baseDsId)
					.knnJoin(gcInfo.name(), baseDsId, -1, m_params.radius(), outputCols)
					.store(joinDsId, FORCE(gcInfo))
					.build();
		m_marmot.execute(plan);

		String outCountDsId = tempDir + SUFFIX_OUT_CNT;
		String filterExpr = String.format("%s != %s", m_idCol, m_clusterCol);
		plan = Plan.builder()
					.load(joinDsId)
					.filter(filterExpr)
					.aggregateByGroup(Group.ofKeys(m_idCol), COUNT().as("out_count"))
					.store(outCountDsId, FORCE)
					.build();
		m_marmot.execute(plan);

		String inCountDsId = tempDir + SUFFIX_IN_CNT;
		plan = Plan.builder()
					.load(joinDsId)
					.filter(filterExpr)
					.aggregateByGroup(Group.ofKeys(m_clusterCol), COUNT().as("in_count"))
					.project(String.format("%s as %s,*-{%s}", m_clusterCol, m_idCol, m_clusterCol))
					.store(inCountDsId, FORCE)
					.build();
		m_marmot.execute(plan);

		String stepDsIdPrefix = tempDir + SUFFIX_STEP;
		plan = Plan.builder()
					.load(joinDsId)
					.takeByGroup(Group.ofKeys(m_idCol).orderBy(m_clusterCol + ":asc"), 1)
					.store(stepDsIdPrefix + "_00000", FORCE)
					.build();
		m_marmot.execute(plan);
		String clusterDsId = iterateToFindClusterId(stepDsIdPrefix);

		int minCount = m_params.minCount();
		String declExpr = String.format("%s:string", m_params.typeColumn());
		String form = "if ( out_count >= %d && in_count >= %d ) { %s = 'CORE'; } "
						+ "else if ( in_count >= %s ) { %s = 'BOARDER'; } "
						+ "else { %s = 'NOISE'; }";
		String updateExpr = String.format(form, minCount, minCount,
											m_typeCol, minCount, m_typeCol, m_typeCol);
		plan = Plan.builder()
					.load(m_params.inputDataSet())
					.hashJoin(m_idCol, clusterDsId, m_idCol,
								String.format("left.*,right.{%s}", m_clusterCol), INNER_JOIN)
					.hashJoin(m_idCol, outCountDsId, m_idCol, "left.*,right.out_count", INNER_JOIN)
					.hashJoin(m_idCol, inCountDsId, m_idCol, "left.*,right.in_count", INNER_JOIN)
					.defineColumn(declExpr)
					.update(updateExpr)
					.project("*-{in_count, out_count}")
					.store(m_params.outputDataSet(), FORCE(gcInfo))
					.build();
		m_marmot.execute(plan);
		
		m_marmot.deleteDir(tempDir);
	}
	
	private String iterateToFindClusterId(String stepDsIdPrefix) {
		Plan plan;
		
		int i = 0;
		while ( true ) {
			String inDsId = String.format("%s_%05d", stepDsIdPrefix, i);
			String outDsId = String.format("%s_%05d", stepDsIdPrefix, i+1);
			Path outPath = new Path(outDsId, UUID.randomUUID().toString());
			String outCols = String.format("left.{%s,%s as interm_id}, right.%s",
											m_idCol, m_clusterCol, m_clusterCol);
			plan = Plan.builder()
						.load(inDsId)
						.project(m_idCol + "," + m_clusterCol)
						.hashJoin(m_clusterCol, inDsId, m_idCol, outCols, INNER_JOIN)
						.tee(outPath.toString(), MarmotFileWriteOptions.FORCE)
						.filter("interm_id != " + m_clusterCol)
						.aggregate(COUNT())
						.build();
			long nupdateds = m_marmot.executeToLong(plan).get();
			m_marmot.buildDataSet(outDsId, outPath.toString(), null, BindDataSetOptions.FORCE(true));
//			System.out.println(nupdateds);
			if ( nupdateds == 0 ) {
				return outDsId;
			}
			
			m_marmot.deleteDataSet(inDsId);
			++i;
		}
	}
}