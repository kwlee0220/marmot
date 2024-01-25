package com.forcewave.ghdfs.util;

import java.io.UnsupportedEncodingException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.EndianUtils;

import com.forcewave.ghdfs.GHDFSConstants;
import com.forcewave.ghdfs.GHDFSConstants.GHDFSColumnType;


public class GHDFSUtil {
	final public static int BYTE_BITS = 8;
	final public static int BYTE_SIZE 		= Byte.SIZE / BYTE_BITS;
	final public static int SHORT_SIZE 		= Short.SIZE / BYTE_BITS;
	final public static int INT_SIZE 		= Integer.SIZE / BYTE_BITS;
	final public static int FLOAT_SIZE 		= Float.SIZE / BYTE_BITS;
	final public static int LONG_SIZE 		= Long.SIZE / BYTE_BITS;
	final public static int DOUBLE_SIZE 	= Double.SIZE / BYTE_BITS;

	public static final String CHARSET = System.getProperty("use.charset", "UTF-8");
	
	public static String encodeColumn(Object indata, String column_type) throws UnsupportedEncodingException   {

		if(!GHDFSColumnType.COLUMN_TYPE_STRING.equals(column_type)
				&& indata == null) {
			return GHDFSConstants.NULL_COLUMN_FLAG;
		}

		byte[] output = null;
		switch(column_type) {
		case GHDFSColumnType.COLUMN_TYPE_INT:
			output = new byte[INT_SIZE];
			EndianUtils.writeSwappedInteger(output, 0,((Integer)indata));
			java.util.Base64.getEncoder().encodeToString(output);
			return java.util.Base64.getEncoder().encodeToString(output);
		case GHDFSColumnType.COLUMN_TYPE_SHORT:
			output = new byte[SHORT_SIZE];
			EndianUtils.writeSwappedShort(output, 0,((Short)indata));
			return java.util.Base64.getEncoder().encodeToString(output);
		case GHDFSColumnType.COLUMN_TYPE_LONG:
			output = new byte[LONG_SIZE];
			EndianUtils.writeSwappedLong(output, 0,((Long)indata));
			return java.util.Base64.getEncoder().encodeToString(output);
		case GHDFSColumnType.COLUMN_TYPE_STRING:
			if(indata == null) {
				return GHDFSConstants.NULL_COLUMN_FLAG;
			}
			if(((String)indata).isEmpty()) {
				return GHDFSConstants.EMPTY_COLUMN_FLAG;
			}
			return java.util.Base64.getEncoder().encodeToString(((String) indata).getBytes(CHARSET));
		case GHDFSColumnType.COLUMN_TYPE_FLOAT:
			output = new byte[FLOAT_SIZE];
			EndianUtils.writeSwappedFloat(output, 0,((Float)indata));
			return java.util.Base64.getEncoder().encodeToString(output);
		case GHDFSColumnType.COLUMN_TYPE_DOUBLE:
			output = new byte[DOUBLE_SIZE];
			EndianUtils.writeSwappedDouble(output, 0,((Double)indata));
			return java.util.Base64.getEncoder().encodeToString(output);
		case GHDFSColumnType.COLUMN_TYPE_BINARY:
			return java.util.Base64.getEncoder().encodeToString((byte[])indata);
		default :
			throw new RuntimeException("Error - not supported column type : " +  column_type);
		}
	}
	
	
	public static Object decodeColumn(String inStr, String column_type) throws UnsupportedEncodingException {
		if(!GHDFSColumnType.COLUMN_TYPE_STRING.equals(column_type)
				&& GHDFSConstants.NULL_COLUMN_FLAG.equals(inStr)) {
			return null;
		}
		byte[] buffer =  Base64.decodeBase64(inStr);;
		return decodeColumn(buffer, column_type);
	}


	private static Object decodeColumn(byte[] indata, String column_type) throws UnsupportedEncodingException {

		switch(column_type) {
		case GHDFSColumnType.COLUMN_TYPE_INT:
			return EndianUtils.readSwappedInteger(indata, 0);
		case GHDFSColumnType.COLUMN_TYPE_SHORT:
			return EndianUtils.readSwappedShort(indata, 0);
		case GHDFSColumnType.COLUMN_TYPE_LONG:
			return EndianUtils.readSwappedLong(indata, 0);
		case GHDFSColumnType.COLUMN_TYPE_STRING:
			String chker = new String(indata, CHARSET);
			if (GHDFSConstants.EMPTY_COLUMN_FLAG.equals(chker)) {
				return "";
			} else if (GHDFSConstants.NULL_COLUMN_FLAG.equals(chker)) {
				return null;
			} else {
				return new String(indata, CHARSET);
			}
		case GHDFSColumnType.COLUMN_TYPE_FLOAT:
			return EndianUtils.readSwappedFloat(indata, 0);
		case GHDFSColumnType.COLUMN_TYPE_DOUBLE:
			return EndianUtils.readSwappedDouble(indata, 0);
		case GHDFSColumnType.COLUMN_TYPE_BINARY:
			return indata;
		default :
			throw new RuntimeException("Error - not supported column type : " +  column_type);
		}
	}
}
