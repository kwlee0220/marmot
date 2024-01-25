package com.forcewave.ghdfs.writer;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;

import com.forcewave.ghdfs.GHDFSConstants;
import com.forcewave.ghdfs.obj.GMetaConstants.CommonConst;
import com.forcewave.ghdfs.obj.GMetaObject;
import com.forcewave.ghdfs.obj.GObject;

public class GHDFSWriter {

	private FileSystem fs = null;
	private GMetaObject meta = null;
	
	private FSDataOutputStream dataOutStream = null;
	private int maxBufferSize = GHDFSConstants.WRITE_BUFF_SIZE; 

	private long totalCount = 0;
	private Envelope totalEnvelope = null;

	private ArrayList<String> writeList = null;
	private int listSize = 0;
	private Envelope listEnv = null;

	
	public GHDFSWriter(FileSystem fs, GMetaObject meta) {
		this(fs, meta,  GHDFSConstants.WRITE_BUFF_SIZE);
	}
	
	public GHDFSWriter(FileSystem fs, GMetaObject meta, int bufferSize) {
		this.fs = fs;
		this.meta = meta;
		this.maxBufferSize = bufferSize;

		writeList = new ArrayList<String>();
		listSize = 0;
	}
	
	public void create(boolean overwrite) throws IOException, ConfigurationException {
		Path layerPath = new Path(CommonConst.DEFAULT_DIR + "/" + meta.getLayerName());
		if(fs.exists(layerPath)) {
			if(overwrite) {
				boolean rtn = fs.delete(layerPath, true);
				if(!rtn) {
					throw new IOException("Error - Cannot create layer  : cannot delete previous layer path - " + layerPath);
				}
			}
			else {
				throw new IOException("Error - Cannot create layer  : layer is already exist : " + layerPath);
			}
		}
		
		fs.mkdirs(layerPath);
		
		// create data output file
		Path dataPath = new Path(layerPath.toString() + "/" + meta.getLayerName() + ".data");
		dataOutStream = fs.create(dataPath, true, 32*1024*1024);
		
		// create properties file
		writeMeta();
	}
	
	protected void writeMeta() throws IOException {
		Path layerPath = new Path(CommonConst.DEFAULT_DIR + "/" + meta.getLayerName());
		Path propPath = new Path(layerPath.toString() + "/" + meta.getLayerName() + ".properties");
		FSDataOutputStream out = fs.create(propPath, true);
		
		PropertiesConfiguration prop = meta.toProperties();
		try {
			prop.save(out);
			out.close();
		}
		catch(Exception e) {
			throw new IOException(e);
		}
	}
	
	public void write(GObject obj) throws IOException {
		String result = GObject.serialize(obj, meta);
		
		if(this.maxBufferSize <= (listSize +  result.length())) {
			writeBufferList();
		}

		this.writeList.add(result + "\n");
		
		listSize += result.length();
		
		if(listEnv == null) {
			listEnv = obj.getEnvelope();
		}
		else {
			listEnv.expandToInclude(obj.getEnvelope());
		}
	}
	
	protected void writeBufferList() throws IOException {
		for(String output : this.writeList) {
			dataOutStream.write(output.getBytes(GHDFSConstants.CHARSET));
		}
		
		dataOutStream.hflush();
		dataOutStream.hsync();
		
		//update meta
		totalCount += writeList.size();
		
		if(totalEnvelope == null) {
			totalEnvelope = listEnv;
		}
		else {
			totalEnvelope.expandToInclude(listEnv);
		}
		
		
		//update meta 
		this.meta.setTotalCount(totalCount);
		this.meta.setEnvelope(totalEnvelope);
		writeMeta();
		
		//init buffer list
		listSize = 0;
		listEnv = null;
		writeList = new ArrayList<String>();
	}
	
	public void close() throws IOException {
		if(this.writeList.size() != 0) {
			writeBufferList();
			listSize = 0;
			listEnv = null;
			writeList = new ArrayList<String>();
		}
		
		if (this.dataOutStream != null) {
			dataOutStream.hflush();
			dataOutStream.close();
		}
	}
	
}
