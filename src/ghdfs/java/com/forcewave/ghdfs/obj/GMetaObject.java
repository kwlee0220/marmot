package com.forcewave.ghdfs.obj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.forcewave.ghdfs.obj.GMetaConstants.CommonConst;
import com.forcewave.ghdfs.obj.GMetaConstants.PropertyConst;

public class GMetaObject {

	private ArrayList<Pair<String, String>> columnInfo = null;

	// layer type (point, line, polygon)
	private String spatialType = null;

	// geometry column index
	private int spatialColumnIdx = -1;

	// layer total record count
	private long totalCount = -1;

	// layer total envelope
	private Envelope envelope = null;

	// layer coordinate system
	private CoordinateReferenceSystem crs = null;

	private String layerName = null;
	/* read from hdfs 
	 *    
	Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inFile = new Path(path);
    FSDataInputStream in = fs.open(inFile);

    PropertiesConfiguration config = new PropertiesConfiguration();
    config.load(in);

    in.close();
	 */
	
	public GMetaObject(String layerName) throws FactoryException {
		setLayerName(layerName);
		
//		Path propPath = new Path(CommonConst.DEFAULT_DIR + "/" + this.layerName + "/" + this.layerName + ".properties");
//		FSDataInputStream in = null;
//	    try {
//			in = fs.open(propPath);
//		} catch (IOException e) {
//			throw new RuntimeException(e);
//		}
//
//	    PropertiesConfiguration config = new PropertiesConfiguration();
//	    try {
//			config.load(in);
//		} catch (ConfigurationException e) {
//			throw new RuntimeException(e);
//		}
//	    
//	    try {
//			in.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//	    
//	    parseProperties(config);
	    
	}
	
	public String getColumnName(int idx) {
		return columnInfo.get(idx).getLeft();
	}
	
	public String getColumnType(int idx) {
		return columnInfo.get(idx).getRight();
	}

	//columnName MUST be lower case
	public int getColumnIndexByName(String columnName) {
		if(columnName == null || columnName.isEmpty()) {
			return -1;
		}
		
		String temp = columnName.trim().toLowerCase();
		
		for(int i = 0 ; i < columnInfo.size() ; i++) {
			Pair<String, String> item = columnInfo.get(i);
			if(item.getLeft().equals(temp)) {
				return i;
			}
		}
		
		return -1;
	}

	
	public String getLayerName() {
		return layerName;
	}


	protected void setLayerName(String layerName) {
		if(layerName == null || layerName.isEmpty()) {
			throw new RuntimeException("Error - layerName is null or empty");
		}

		// layername must be lower case
		//  => upper case String covert lower case String
		this.layerName = layerName.trim().toLowerCase();
	}


	public GMetaObject(PropertiesConfiguration config) throws FactoryException {
		parseProperties(config);
	}

	protected void parseProperties(PropertiesConfiguration config) throws FactoryException {
		spatialType = config.getString(PropertyConst.PROP_KEY_TYPE, "");

		String[] env_str = config.getStringArray(PropertyConst.PROP_KEY_ENV);
		if (env_str == null || env_str.length == 0) {
			envelope = null;
		} else if (env_str.length != 4) {
			String temp = config.getString(PropertyConst.PROP_KEY_ENV);
			throw new RuntimeException("Error - In Property, Envelope value is incorrect - " + temp);
		} else {
			double minX = Double.parseDouble(env_str[0]);
			double maxX = Double.parseDouble(env_str[1]);
			double minY = Double.parseDouble(env_str[2]);
			double maxY = Double.parseDouble(env_str[3]);
			envelope = new Envelope(minX, maxX, minY, maxY);
		}

		totalCount = config.getInt(PropertyConst.PROP_KEY_TOTALCNT, -1);

		this.setCrs(config.getString(PropertyConst.PROP_KEY_WKTCRS, ""));

		spatialColumnIdx = config.getInt(PropertyConst.PROP_KEY_SPATIAL_IDX, -1);

		List<String> nameList = config.getList(PropertyConst.PROP_KEY_COL_NAME);
		if (nameList == null || nameList.size() == 0) {
			throw new RuntimeException("Error - In Property, Column Name List is empty ");
		}
		List<String> typeList = config.getList(PropertyConst.PROP_KEY_COL_TYPE);

		if (typeList == null || typeList.size() == 0) {
			throw new RuntimeException("Error - In Property, Column Type List is empty ");
		}

		if (nameList.size() != typeList.size()) {
			throw new RuntimeException(
					"Error - In Property, Column Type & Name List is incorrect : list size is differnt");
		}

		columnInfo = new ArrayList<Pair<String, String>>();
		for (int i = 0; i < nameList.size(); i++) {
			columnInfo.add(Pair.of((String) nameList.get(i), (String) typeList.get(i)));
		}
	}
	
	public int getColumnCount() {
		return columnInfo.size();
	}
	
	public ArrayList<Pair<String, String>> getColumnInfo() {
		return columnInfo;
	}

	public void setColumnInfo(ArrayList<Pair<String, String>> columnInfo, int spatialColumnIdx) {
		this.columnInfo = columnInfo;
		this.spatialColumnIdx = spatialColumnIdx;
	}

	public String getSpatialType() {
		return spatialType;
	}

	public void setSpatialType(String spatialType) {
		this.spatialType = spatialType;
	}

	public int getSpatialColumnIdx() {
		return spatialColumnIdx;
	}

	public long getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(long totalCount) {
		this.totalCount = totalCount;
	}

	public Envelope getEnvelope() {
		return envelope;
	}

	public void setEnvelope(Envelope envelope) {
		if (envelope == null || envelope.isNull()) {
			this.envelope = null;
		}
		this.envelope = envelope;
	}

	public CoordinateReferenceSystem getCrs() {
		return crs;
	}

	public String getWKTCrs() {
		if (crs == null) {
			return null;
		}
		return crs.toWKT();
	}

	public void setCrs(CoordinateReferenceSystem crs) {
		this.crs = crs;
	}

	public void setCrs(String wktCrs) throws FactoryException {
		this.crs = CRS.parseWKT(wktCrs);
	}

	public PropertiesConfiguration toProperties() {
		
		PropertiesConfiguration config = new PropertiesConfiguration();
		config.setListDelimiter('|');

		config.setProperty(PropertyConst.PROP_KEY_TYPE, spatialType);

		if (envelope == null || envelope.isNull()) {
			config.setProperty(PropertyConst.PROP_KEY_ENV, null);
			
		} else {
			double buff[] = new double[4];
			buff[0] = envelope.getMinX();
			buff[1] = envelope.getMaxX();
			buff[2] = envelope.getMinY();
			buff[3] = envelope.getMaxY();
			config.setProperty(PropertyConst.PROP_KEY_ENV, buff);
		}

		config.setProperty(PropertyConst.PROP_KEY_TOTALCNT, totalCount);
		config.setProperty(PropertyConst.PROP_KEY_WKTCRS, this.getWKTCrs());
		config.setProperty(PropertyConst.PROP_KEY_SPATIAL_IDX, spatialColumnIdx);

		if (columnInfo == null || columnInfo.size() == 0) {
			throw new RuntimeException("Error - Column List is empty");
		}
		List<String> nameList = new ArrayList<String>();
		List<String> typeList = new ArrayList<String>();

		for (int i = 0; i < columnInfo.size(); i++) {
			Pair<String, String> obj = columnInfo.get(i);
			nameList.add(obj.getLeft());
			typeList.add(obj.getRight());
		}
		
		config.setProperty(PropertyConst.PROP_KEY_COL_NAME, nameList);
		config.setProperty(PropertyConst.PROP_KEY_COL_TYPE, typeList);
		
		return config;
	}
	
	@Override
	public String toString() {
		String rtn = "";
		rtn += "Layer Name : " + this.layerName + "\n";
		rtn += "Layer Spatial Type : " + this.getSpatialType() + "\n";
		rtn += "Layer Envelope : " + this.envelope + "\n";
		rtn += "CRS : " + this.getWKTCrs() + "\n";
		rtn += "Total Count : " + this.totalCount + "\n";
		rtn += "Column Infomation" + " \n";
		for(int i = 0 ; i < this.getColumnCount() ; i++) {
			rtn += " [" + i + "]" + " : " + this.getColumnName(i) + " " + this.getColumnType(i) + "\n";
		}
		rtn += "Spatial Colmun Index : " + this.getSpatialColumnIdx() + "\n";

		return rtn;
	}
	
	public static GMetaObject loadFromHDFS(FileSystem fs, String layerName) throws FactoryException {
		GMetaObject meta = new GMetaObject(layerName);
		meta.setLayerName(layerName);

		Path propPath = new Path(CommonConst.DEFAULT_DIR + "/" + meta.getLayerName() + "/" + meta.getLayerName() + ".properties");
		FSDataInputStream in = null;
	    try {
			in = fs.open(propPath);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	    PropertiesConfiguration config = new PropertiesConfiguration();
	    config.setListDelimiter('|');
	    try {
			config.load(in);
		} catch (ConfigurationException e) {
			throw new RuntimeException(e);
		}
	    
	    try {
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	    
	    meta.parseProperties(config);
	    
	    return meta;
	}

}
