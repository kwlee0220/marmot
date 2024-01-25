package marmot.geowave;

import com.forcewave.ghdfs.GHDFSConstants;
import com.forcewave.ghdfs.obj.GMetaConstants;

import marmot.type.DataType;
import marmot.type.GeometryDataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class GHdfsUtils {
	static String toPath(String layerName) {
		return String.format("/tmp/ghdfs/%s/%s.data", layerName, layerName);
	}
	
	static GeometryDataType fromGhdfsSpatialType(String gstype) {
		switch ( gstype ) {
			case GMetaConstants.CommonConst.TYPE_POINT:
				return DataType.POINT;
			case GMetaConstants.CommonConst.TYPE_POLYGON:
				return DataType.POLYGON;
			case GMetaConstants.CommonConst.TYPE_LINE:
				return DataType.LINESTRING;
				
			default:
				throw new IllegalArgumentException("unknown spatial data type: " + gstype);
		}
	}
	
	static String toGhdfsSpatialType(DataType type) {
		switch ( type.getTypeCode() ) {
			case POINT:
				return GMetaConstants.CommonConst.TYPE_POINT;
			case POLYGON:
				return GMetaConstants.CommonConst.TYPE_POLYGON;
			case LINESTRING:
				return GMetaConstants.CommonConst.TYPE_LINE;
				
			default:
				throw new IllegalArgumentException("unsupported geometry data type: " + type);
		}
	}
	
	static DataType fromGhdfsColumnType(String gtype) {
		switch ( gtype ) {
			case GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_STRING:
				return DataType.STRING;
			case GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_DOUBLE:
				return DataType.DOUBLE;
			case GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_INT:
				return DataType.INT;
			case GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_BINARY:
				return DataType.BINARY;
			case GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_LONG:
				return DataType.LONG;
			default:
				throw new IllegalArgumentException("unknown GHDFSColumnType: " + gtype);
		}
	}
	
	static String toGhdfsColumnType(DataType type) {
		switch ( type.getTypeCode() ) {
			case POINT:
			case POLYGON:
			case LINESTRING:
				return GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_BINARY;
			case STRING:
				return GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_STRING;
			case DOUBLE:
				return GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_DOUBLE;
			case INT:
				return GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_INT;
			case LONG:
				return GHDFSConstants.GHDFSColumnType.COLUMN_TYPE_LONG;
			default:
				throw new IllegalArgumentException("unsupported ghdfs data type: " + type);
		}
	}
}
