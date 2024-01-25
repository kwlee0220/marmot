package marmot.optor.geo.join;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSetImpl;
import marmot.io.geo.cluster.CacheableQuadCluster;
import marmot.io.geo.cluster.QuadClusterFile;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.geo.SpatialRelation;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.JoinUtils;
import marmot.optor.support.colexpr.ColumnSelector;
import marmot.plan.SpatialJoinOptions;
import marmot.proto.optor.SpatialBlockJoinProto;
import marmot.support.PBSerializable;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialBlockJoin extends AbstractRecordSetFunction
								implements PBSerializable<SpatialBlockJoinProto> {
	public static final Logger s_logger = LoggerFactory.getLogger(SpatialBlockJoin.class);

	private final String m_inputGeomCol;
	private final String m_innerDsId;
	private final SpatialJoinOptions m_options;
	
	private final SpatialRelation m_joinExpr;

	private DataSetImpl m_innerDs;
	private QuadClusterFile<? extends CacheableQuadCluster> m_innerIdxFile;
	private ColumnSelector m_selector;
	
	public SpatialBlockJoin(String inputGeomCol, String innerDsId, SpatialJoinOptions opts) {
		m_innerDsId = innerDsId;
		m_inputGeomCol = inputGeomCol;
		m_options = opts;
		
		m_joinExpr = opts.joinExpr().map(SpatialRelation::parse)
							.getOrElse(SpatialRelation.INTERSECTS);
	}
	
	public boolean isStreamable() {
		return false;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_innerDs = marmot.getDataSet(m_innerDsId);
		
		try {
			m_selector = JoinUtils.createJoinColumnSelector(inputSchema, m_innerDs.getRecordSchema(),
															m_options.outputColumns());
			RecordSchema outSchema = m_selector.getRecordSchema();
			setInitialized(marmot, inputSchema, outSchema);
		}
		catch ( Exception e ) {
 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s", this, e));
		}
	}
	
	@Override
	public RecordSet apply(RecordSet outerRSet) {
		checkInitialized();
		
		m_innerIdxFile = m_innerDs.loadOrBuildQuadClusterFile(m_marmot);
		return new ClusteredNLJoinRecordSet(m_inputGeomCol, outerRSet, m_innerIdxFile, m_options);
	}
	
	@Override
	public String toString() {
		return String.format("%s: inner=%s,join=%s", getClass().getSimpleName(),
								m_innerDsId, m_joinExpr);
	}

	public static SpatialBlockJoin fromProto(SpatialBlockJoinProto proto) {
		SpatialJoinOptions opts = SpatialJoinOptions.fromProto(proto.getOptions());
		return new SpatialBlockJoin(proto.getGeomColumn(), proto.getParamDataset(), opts);
	}

	@Override
	public SpatialBlockJoinProto toProto() {
		return SpatialBlockJoinProto.newBuilder()
									.setGeomColumn(m_inputGeomCol)
									.setParamDataset(m_innerDsId)
									.setOptions(m_options.toProto())
									.build();
	}
}
