package com.forcewave.ghdfs.reader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.opengis.referencing.FactoryException;

import com.forcewave.ghdfs.GHDFSConstants;
import com.forcewave.ghdfs.obj.GMetaConstants.CommonConst;
import com.forcewave.ghdfs.obj.GMetaObject;
import com.forcewave.ghdfs.obj.GObject;

public class GHDFSReader {

	private FileSystem fs = null;
	private GMetaObject meta = null;
	private String layerName = null;

	private BufferedReader dataReader = null;

	private BlockingQueue<GObject> readQueue = null;
	private GObjectProducer objReader = null;
	private Thread readThread = null;

	private int readCount = 0;

	public GHDFSReader(FileSystem fs, String layerName) {
		this(fs, layerName, GHDFSConstants.WRITE_BUFF_SIZE);
	}

	protected GHDFSReader(FileSystem fs, String layerName, /* deprecated */ int bufferSize) {
		this.fs = fs;
		this.layerName = layerName;
		// this.maxBufferSize = bufferSize;

		readQueue = new ArrayBlockingQueue<GObject>(1000);
	}

	public void open() throws IOException {
		Path layerPath = new Path(CommonConst.DEFAULT_DIR + "/" + this.layerName);
		if (!fs.exists(layerPath)) {
			throw new IOException("Error - Cannot open layer  : layer path is not exist : " + layerPath);
		}

		Path dataPath = new Path(CommonConst.DEFAULT_DIR + "/" + this.layerName + "/" + this.layerName + ".data");
		if (!fs.exists(dataPath)) {
			throw new IOException("Error - Cannot open layer  : layer file is not exist : " + dataPath);
		}

		Path propPath = new Path(CommonConst.DEFAULT_DIR + "/" + this.layerName + "/" + this.layerName + ".properties");
		if (!fs.exists(propPath)) {
			throw new IOException("Error - Cannot open layer  : layer properties is not exist : " + propPath);
		}

		try {
			this.meta = readMeata();
		} catch (FactoryException e) {
			throw new RuntimeException(e);
		}

		FSDataInputStream dataInputStream = fs.open(dataPath);
		this.dataReader = new BufferedReader(new InputStreamReader(dataInputStream), 32 * 1024 * 1024);

	}

	public GObject read() {
		GObject obj = null;

		if (objReader == null) {
			objReader = new GObjectProducer(readQueue, this.meta, this.dataReader);
			readThread = new Thread(objReader);
			readThread.start();

			// waiting for initial data reading
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		// END of File
		if (readQueue.isEmpty() && objReader.isEOF()) {
			return null;
		}

		try {
			obj = readQueue.take();
			readCount++;
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}

		return obj;
	}

	public GMetaObject getMeta() {
		return this.meta;
	}

	protected GMetaObject readMeata() throws FactoryException {
		return GMetaObject.loadFromHDFS(this.fs, this.layerName);
	}

	public String getLayerName() {
		return this.layerName;
	}

	public class GObjectProducer implements Runnable {

		private BlockingQueue<GObject> queue;
		private BufferedReader reader;
		private GMetaObject meta;
		private boolean eof;

		public GObjectProducer(BlockingQueue<GObject> queue, GMetaObject meta, BufferedReader reader) {
			this.queue = queue;
			this.reader = reader;
			this.meta = meta;
		}

		@Override
		public void run() {
			String line = null;
			try {
				while ((line = reader.readLine()) != null) {

					if (!line.isEmpty()) {
						GObject obj = GObject.deserialize(line, meta);
						queue.put(obj);
					}
				}
			} catch (IOException e) {
				throw new RuntimeException(e);
			} catch (InterruptedException e) {

			}
			eof = true;
		}

		public boolean isEOF() {
			return this.eof;
		}
	}

	public void close() {
		if (this.readThread != null && readThread.isAlive()) {
			this.readThread.interrupt();
		}

		try {
			if (dataReader != null) {
				this.dataReader.close();
			}
		} catch (IOException e) {
		}
	}
}
