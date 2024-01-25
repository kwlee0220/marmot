package marmot.optor.geo.cluster;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CoordinateTransform;
import marmot.optor.FlatTransform;
import marmot.optor.support.QuadKeyBinder;
import marmot.optor.support.QuadKeyBinder.QuadKeyBinding;
import marmot.proto.optor.AttachQuadKeyProto;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.CSV;
import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AttachQuadKey extends FlatTransform implements PBSerializable<AttachQuadKeyProto> {
	public static final String COL_QUAD_KEY = Constants.COL_QUAD_KEY;
	public static final String COL_MBR = Constants.COL_MBR;
	public static final String OUTLIER_QUADKEY = Constants.QUADKEY_OUTLIER;
	private static final Envelope EMPTY_BOUNDS = new Envelope();

	private final GeometryColumnInfo m_gcInfo;
	private final Set<String> m_qkeys;
	private final FOption<Envelope> m_validRange;
	private final boolean m_bindOutlier;
	private final boolean m_bindOnlyToOwner;	// 주어진 공간정보가 포함되는 모든 quad-key를 포함시킬지
												// 아니면 owner quad-key만 포함시킬 지 결정 

	private QuadKeyBinder m_binder;
	private int m_geomIdx;
	private int m_quadKeyColIdx;
	private int m_mbrColIdx;
	@Nullable private CoordinateTransform m_trans;
	
	public AttachQuadKey(GeometryColumnInfo gcInfo, Set<String> qkeys, FOption<Envelope> validRange,
							boolean bindOutlier, boolean bindOnlyToOwner) {
		Utilities.checkNotNullArgument(gcInfo, "GeometryColumnInfo");
		Utilities.checkNotNullArgument(qkeys, "QueryKey list");
		
		m_gcInfo = gcInfo;
		m_qkeys = qkeys;
		m_validRange = validRange;
		m_bindOutlier = bindOutlier;
		m_bindOnlyToOwner = bindOnlyToOwner;
		m_binder = new QuadKeyBinder(m_qkeys, m_bindOutlier, m_bindOnlyToOwner);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema =inputSchema.toBuilder() 
											.addOrReplaceColumn(COL_QUAD_KEY, DataType.STRING)
											.addOrReplaceColumn(COL_MBR, DataType.ENVELOPE)
											.build();
		
		m_geomIdx = inputSchema.getColumn(m_gcInfo.name()).ordinal();
		m_quadKeyColIdx = outSchema.getColumn(COL_QUAD_KEY).ordinal();
		m_mbrColIdx = outSchema.getColumn(COL_MBR).ordinal();
		m_trans = CoordinateTransform.getTransformToWgs84(m_gcInfo.srid());
		
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	@Override
	public RecordSet transform(Record input) {
		Geometry geom = input.getGeometry(m_geomIdx);
		if ( geom == null || geom.isEmpty() ) {
			return RecordSet.of(toOutputRecord(input, OUTLIER_QUADKEY, EMPTY_BOUNDS));
		}
		
		Envelope mbr = geom.getEnvelopeInternal();
		if ( !m_validRange.map(range -> mbr.intersects(range)).getOrElse(true) ) {
			return RecordSet.of(toOutputRecord(input, OUTLIER_QUADKEY, EMPTY_BOUNDS));
		}
		
		Envelope mbr84 = toWgs84(mbr);
		List<QuadKeyBinding> bindings = m_binder.bindQuadKeys(mbr84);
		if ( bindings.size() > 0 ) {
			FStream<Record> rstream = FStream.from(bindings)
											.map(binding -> toOutputRecord(input, binding.quadkey(),
																			binding.mbr4326()));
			return RecordSet.from(getRecordSchema(), rstream);
		}
		else {
			return RecordSet.of(toOutputRecord(input, OUTLIER_QUADKEY, EMPTY_BOUNDS));
		}
	}
	
	@Override
	public String toString() {
		String qkeysStr = FStream.from(m_qkeys).join(',');
		if ( qkeysStr.length() > 40 ) {
			qkeysStr = qkeysStr.substring(0, 40) + "...";
		}
		return String.format("%s: geom=%s, quadkeys=%s", getClass().getSimpleName(),
							m_gcInfo, qkeysStr);
	}

	public static AttachQuadKey fromProto(AttachQuadKeyProto proto) {
		GeometryColumnInfo geomColInfo = GeometryColumnInfo.fromProto(proto.getGeometryColumnInfo());
		Set<String> qkeys = CSV.parseCsv(proto.getQuadKeys()).toSet();
		
		FOption<Envelope> validRange;
		switch ( proto.getOptionalValidRangeCase() ) {
			case VALID_RANGE:
				validRange = FOption.of(PBUtils.fromProto(proto.getValidRange()));
				break;
			case OPTIONALVALIDRANGE_NOT_SET:
				validRange = FOption.empty();
				break;
			default:
				throw new AssertionError();
		}
		
		return new AttachQuadKey(geomColInfo, qkeys, validRange, proto.getBindOutlier(),
								proto.getBindOnce());
	}

	@Override
	public AttachQuadKeyProto toProto() {
		String qkeysStr = FStream.from(m_qkeys).join(',');
		AttachQuadKeyProto.Builder builder = AttachQuadKeyProto.newBuilder()
															.setQuadKeys(qkeysStr)
															.setGeometryColumnInfo(m_gcInfo.toProto())
															.setBindOutlier(m_bindOutlier)
															.setBindOnce(m_bindOnlyToOwner);
		m_validRange.map(PBUtils::toProto).ifPresent(builder::setValidRange);
		return builder.build();
	}
	
	private Record toOutputRecord(Record input, String quadKey, Envelope mbr) {
		Record output = DefaultRecord.of(getRecordSchema());
		output.set(input);
		output.set(m_quadKeyColIdx, quadKey);
		output.set(m_mbrColIdx, mbr);
		
		return output;
	}
	
	private Envelope toWgs84(Envelope envl) {
		return (m_trans != null) ? m_trans.transform(envl) : envl;
	}
}
