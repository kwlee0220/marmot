package com.forcewave.ghdfs.obj;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;

import com.forcewave.ghdfs.util.GHDFSUtil;

public class GObject {
	private ArrayList<Object> columnList = null;
	private Envelope env = null;

	public GObject(ArrayList<Object> list, Envelope env) {
		this.columnList = list;
		this.env = env;
	}

	public Envelope getEnvelope() {
		return env;
	}

	public Object getColumnItem(int idx) {
		return columnList.get(idx);
	}

	public Object setColumnItem(int idx, Object obj) {
		return columnList.set(idx, obj);
	}

	public int getColumnCount() {
		if (columnList == null) {
			return 0;
		}
		return this.columnList.size();
	}

	public static GObject deserialize(String encodeString, GMetaObject meta) throws UnsupportedEncodingException {
		if (encodeString == null) {
			throw new RuntimeException("Error - Cannot deserialize String : string is null");
		}

		String temp = encodeString.trim();
		if (temp.isEmpty()) {
			throw new RuntimeException("Error - Cannot deserialize String : string is empty");
		}
		
		int cSize = meta.getColumnCount();
		String inStr[] = temp.split(",");
		if (inStr.length != cSize) {
			throw new RuntimeException(
					"Error - Cannot deserialize String : column count is incorrect : below is input string \n " + temp);
		}

		WKBReader wkb = new WKBReader();
		ArrayList<Object> result = new ArrayList<Object>();
		Envelope env = null;
		for (int i = 0; i < cSize; i++) {
			Object obj = GHDFSUtil.decodeColumn(inStr[i], meta.getColumnType(i));
			if(i == meta.getSpatialColumnIdx()) {
				try {
					Geometry geom = wkb.read((byte[]) obj);
					env = geom.getEnvelopeInternal();
					obj = geom;
				} catch (ParseException e) {
					throw new RuntimeException(e);
				}
			}

			result.add(obj);
		}
		
		return new GObject(result, env);
	}

	public static String serialize(GObject object, GMetaObject meta)  {
		if (object == null) {
			throw new RuntimeException("Error - Cannot serialize Object : object is null");
		}

		int cSize = meta.getColumnCount();
		if (object.getColumnCount() != cSize) {
			throw new RuntimeException(
					"Error - Cannot serialize Object : column count is different from meta column count");
		}

		WKBWriter wkb = new WKBWriter();
		
		String result = "";
		for (int i = 0; i < cSize; i++) {
			if (i != 0) {
				result = result + ",";
			}
			String rtn = null;
			Object item = object.getColumnItem(i);
			
			if(i == meta.getSpatialColumnIdx()) {
				item = wkb.write((Geometry) item);
			}
			
			try {
				rtn = GHDFSUtil.encodeColumn(item, meta.getColumnType(i));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
			result = result + rtn;
		}
		return result;
	}

}
