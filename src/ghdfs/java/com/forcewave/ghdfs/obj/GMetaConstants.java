package com.forcewave.ghdfs.obj;

public class GMetaConstants {
	
	public static class CommonConst {
		//hdfs constants
		public static final String DEFAULT_DIR = "/tmp/ghdfs";
		public static final String DEFAULT_USER = "hdfs";

		//layer type
		public static final String TYPE_POINT = "point";
		public static final String TYPE_LINE = "line";
		public static final String TYPE_POLYGON = "polygon";
	}
	
	public static class PropertyConst {
		public static final String PROP_KEY_TYPE = "layer.type";
		public static final String PROP_KEY_ENV = "layer.envelope";
		public static final String PROP_KEY_TOTALCNT = "layer.totalcount";
		public static final String PROP_KEY_WKTCRS = "layer.wktcrs";
		
		public static final String PROP_KEY_SPATIAL_IDX = "column.spatialidx";
		public static final String PROP_KEY_COL_NAME = "column.name";
		public static final String PROP_KEY_COL_TYPE = "column.type";
	}
	

}
