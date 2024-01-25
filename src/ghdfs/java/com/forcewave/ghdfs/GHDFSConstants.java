package com.forcewave.ghdfs;

public class GHDFSConstants {

	
	//encoding/decoding constants
	public static final String CHARSET = System.getProperty("use.charset", "UTF-8");
	public static final String NULL_COLUMN_FLAG = "@";
	public static final String EMPTY_COLUMN_FLAG = "#";

	//columntype constants
	public static class GHDFSColumnType {
		public static final String COLUMN_TYPE_BYTE = "tynyint";
		public static final String COLUMN_TYPE_SHORT = "smallint";
		public static final String COLUMN_TYPE_INT = "int";
		public static final String COLUMN_TYPE_LONG = "bigint";
		public static final String COLUMN_TYPE_FLOAT = "float";
		public static final String COLUMN_TYPE_DOUBLE = "double";
		public static final String COLUMN_TYPE_STRING = "string";
		public static final String COLUMN_TYPE_BINARY = "binary";
	}
	
	public static final int READ_BUFF_SIZE = 128*1024*1024;
	public static final int WRITE_BUFF_SIZE = 128*1024*1024;
	
	
}
