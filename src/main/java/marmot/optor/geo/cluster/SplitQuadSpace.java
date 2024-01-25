package marmot.optor.geo.cluster;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.SplitQuadSpaceProto;
import marmot.support.PBSerializable;
import utils.UnitUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SplitQuadSpace extends AbstractRecordSetFunction
							implements PBSerializable<SplitQuadSpaceProto> {
	public static final String COLUMN_QUADKEY = SampledQuadSpace.COLUMN_QUADKEY;
	public static final String COLUMN_LENGTH = SampledQuadSpace.COLUMN_LENGTH;
	public static RecordSchema SCHEMA = SampledQuadSpace.SCHEMA;

	private final GeometryColumnInfo m_gcInfo;
	final long m_splitSize;
	
	public SplitQuadSpace(GeometryColumnInfo gcInfo, long splitSize) {
		m_gcInfo = gcInfo;
		m_splitSize = splitSize;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, SCHEMA);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		return new SampledQuadSpace(input, m_splitSize);
	}
	
	@Override
	public String toString() {
		return String.format("%s: split_size=%s", getClass().getSimpleName(),
								UnitUtils.toByteSizeString(m_splitSize));
	}
	
	public static SplitQuadSpace fromProto(SplitQuadSpaceProto proto) {
		GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeometryColumnInfo());
		return new SplitQuadSpace(gcInfo, proto.getSplitSize());
	}

	@Override
	public SplitQuadSpaceProto toProto() {
		return SplitQuadSpaceProto.newBuilder()
									.setGeometryColumnInfo(m_gcInfo.toProto())
									.setSplitSize(m_splitSize)
									.build();
	}
}
