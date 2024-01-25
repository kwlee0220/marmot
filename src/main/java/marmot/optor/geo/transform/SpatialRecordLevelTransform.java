package marmot.optor.geo.transform;

import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.optor.support.RecordLevelTransform;
import marmot.plan.GeomOpOptions;
import marmot.type.GeometryDataType;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialRecordLevelTransform<T extends SpatialRecordLevelTransform<T>>
														extends RecordLevelTransform {
	private static final Logger s_logger = LoggerFactory.getLogger(SpatialRecordLevelTransform.class);
	
	private final String m_inputGeomCol;
	protected final GeomOpOptions m_options;

	private int m_inputGeomColIdx = -1;
	protected GeometryDataType m_inputGeomType;
	private String m_outputGeomCol;
	private int m_outputGeomColIdx = -1;
	private boolean m_throwOpError = true;

	abstract protected GeometryDataType initialize(GeometryDataType inGeomType);
	protected GeometryDataType initialize(GeometryDataType inGeomType, RecordSchema inputSchema) {
		return initialize(inGeomType);
	}
	abstract protected Geometry transform(Geometry geom);
	protected Geometry transform(Geometry geom, Record inputRecord) {
		return transform(geom);
	}
	
	protected SpatialRecordLevelTransform(String inGeomCol, GeomOpOptions opts) {
		Utilities.checkNotNullArgument(inGeomCol, "input geometry column is null");
		Utilities.checkNotNullArgument(opts, "GeomOpOptions is null");
		
		m_inputGeomCol = inGeomCol;
		m_options = opts;
		setLogger(s_logger);
	}
	
	public String getInputGeometryColumn() {
		return m_inputGeomCol;
	}
	
	public int getInputGeometryColumnIndex() {
		return m_inputGeomColIdx;
	}
	
	public String getOutputGeometryColumn() {
		return m_options.outputColumn().getOrElse(m_inputGeomCol);
	}
	
	public int getOutputGeometryColumnIndex() {
		return m_outputGeomColIdx;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_inputGeomColIdx = inputSchema.getColumn(m_inputGeomCol).ordinal();

		Column col = inputSchema.getColumnAt(m_inputGeomColIdx);
		GeometryDataType outGeomType = initialize((GeometryDataType)col.type(), inputSchema);
		
		String outGeomCol = m_options.outputColumn().getOrElse(m_inputGeomCol);
		RecordSchema outSchema = inputSchema.toBuilder()
									.addOrReplaceColumn(outGeomCol, outGeomType)
									.build();
		m_outputGeomColIdx = outSchema.getColumn(outGeomCol).ordinal();
		
		m_throwOpError = m_options.throwOpError().getOrElse(false);
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		checkInitialized();
		
		try {
			output.set(input);
			
			Geometry geom = input.getGeometry(m_inputGeomColIdx);
			if ( geom == null || geom.isEmpty() ) {
				output.set(m_outputGeomColIdx, handleNullEmptyGeometry(geom));
			}
			else {
				Geometry transformed = transform(geom, input);
				output.set(m_outputGeomColIdx, transformed);
			}
			
			return true;
		}
		catch ( Exception e ) {
			if ( getLogger().isDebugEnabled() ) {
				getLogger().warn("fails to transform geometry: cause=" + e);
			}
			
			if ( m_throwOpError ) {
				throw e;
			}
			
			output.set(m_outputGeomColIdx, null);
			return true;
		}
	}
	
	protected Geometry handleNullEmptyGeometry(Geometry geom) {
		if ( geom == null ) {
			return null;
		}
		else if ( geom.isEmpty() ) {
			return m_inputGeomType.newInstance();
		}
		else {
			throw new AssertionError("Should not be called: " + getClass());
		}
	}
}
