package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.geo.CRSUtils;
import marmot.geo.CoordinateTransform;
import marmot.plan.GeomOpOptions;
import marmot.proto.optor.TransformCrsProto;
import marmot.support.PBSerializable;
import marmot.type.GeometryDataType;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TransformSrid extends SpatialRecordLevelTransform<TransformSrid>
							implements PBSerializable<TransformCrsProto> {
	static final Logger s_logger = LoggerFactory.getLogger(TransformSrid.class);

	private final String m_srcSrid;
	private final String m_tarSrid;
	private CoordinateTransform m_trans;

	public TransformSrid(String srcGeomCol, String srcSrid, String tarSrid, GeomOpOptions opts) {
		super(srcGeomCol, opts);
		Utilities.checkNotNullArgument(srcSrid, "srcSrid is null");
		Utilities.checkNotNullArgument(tarSrid, "tarSrid is null");
		
		m_srcSrid = srcSrid;
		m_tarSrid = tarSrid;
	}

	@Override
	protected GeometryDataType initialize(GeometryDataType inGeomType) {
		if ( !m_srcSrid.equals(m_tarSrid) ) {
			CoordinateReferenceSystem srcCrs = CRSUtils.toCRS(m_srcSrid);
			CoordinateReferenceSystem tarCrs = CRSUtils.toCRS(m_tarSrid);
			m_trans = CoordinateTransform.get(srcCrs, tarCrs);
		}
		else {
			m_trans = null;
		}
		
		return inGeomType;
	}
	
	@Override
	protected Geometry transform(Geometry src) {
		try {
			return (m_trans != null) ? m_trans.transform(src) : src;
		}
		catch ( Exception e ) {
			return m_inputGeomType.newInstance();
		}
	}
	
	@Override
	public String toString() {
		return String.format("%s: %s(%s)->%s(%s)", getClass().getSimpleName(),
							getInputGeometryColumn(), m_srcSrid,
							getOutputGeometryColumn(), m_tarSrid);
	}

	public static TransformSrid fromProto(TransformCrsProto proto) {
		GeomOpOptions opts = GeomOpOptions.fromProto(proto.getOptions());
		return new TransformSrid(proto.getGeometryColumn(), proto.getSourceSrid(),
								proto.getTargetSrid(), opts);
	}

	@Override
	public TransformCrsProto toProto() {
		return TransformCrsProto.newBuilder()
								.setGeometryColumn(getInputGeometryColumn())
								.setSourceSrid(m_srcSrid)
								.setTargetSrid(m_tarSrid)
								.setOptions(m_options.toProto())
								.build();
	}
}
