package marmot.geowave;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.FactoryException;

import com.forcewave.ghdfs.GHDFSConstants.GHDFSColumnType;
import com.forcewave.ghdfs.obj.GMetaObject;
import com.forcewave.ghdfs.obj.GObject;
import com.forcewave.ghdfs.writer.GHDFSWriter;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.CRSUtils;
import marmot.mapreduce.HdfsWriterTerminal;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.StoreAsGHdfsProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import utils.Throwables;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAsGHdfs extends AbstractRecordSetConsumer
						implements HdfsWriterTerminal, MapReduceJobConfigurer,
									PBSerializable<StoreAsGHdfsProto> {
	private final String m_layerName;
	private final GeometryColumnInfo m_gcInfo;
	private final boolean m_force;
	private final Path m_path;
	
	private int m_geomColIdx = -1;
	
	private StoreAsGHdfs(String layerName, GeometryColumnInfo gcInfo, boolean force) {
		Utilities.checkNotNullArgument(layerName, "layer name is null");
		Utilities.checkNotNullArgument(gcInfo, "GeometryColumnInfo is null");
		
		m_layerName = layerName;
		m_gcInfo = gcInfo;
		m_force = force;
		m_path = new Path(String.format("/tmp/ghdfs/%s", layerName));
	}

	@Override
	public Path getOutputPath() {
		return m_path;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_geomColIdx = inputSchema.getColumn(m_gcInfo.name()).ordinal();
		
		setInitialized(marmot, inputSchema);
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();

		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		
		GHDFSWriter writer = null;
		try {
			writer = newGHDFSWriter(m_marmot.getHadoopConfiguration());
			
			Record record = DefaultRecord.of(getInputRecordSchema());
			while ( rset.next(record) ) {
				GObject gobj = toGObject(record);
				writer.write(gobj);
			}
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			
			Throwables.throwIfInstanceOf(cause, RuntimeException.class);
			throw new RecordSetException(cause);
		}
		finally {
			if ( writer != null ) {
				try {
					writer.close();
				}
				catch ( IOException e ) {
					throw new RecordSetException(e);
				}
			}
		}
	}
	
	@Override
	public String toString() {
		return String.format("store_ghdfs[layer=%s]", m_layerName);
	}
	
	private GObject toGObject(Record record) {
		ArrayList<Object> columnList = record.fstream()
											.toValueStream()
											.toList();
		Envelope bounds = record.getGeometry(m_geomColIdx).getEnvelopeInternal();
		return new GObject(columnList, bounds);
	}
	
	private GHDFSWriter newGHDFSWriter(Configuration conf)
		throws IOException, FactoryException, ConfigurationException {
		GMetaObject gmeta = new GMetaObject(m_layerName);
		gmeta.setCrs(CRSUtils.toCRS(m_gcInfo.srid()));
		
		RecordSchema schema = getInputRecordSchema();
		Column geomCol = schema.getColumn(m_gcInfo.name());
		gmeta.setSpatialType(GHdfsUtils.toGhdfsSpatialType(geomCol.type()));
		
		ArrayList<Pair<String, String>> cinfoList
						= schema.streamColumns()
								.map(col -> Pair.of(col.name(), GHdfsUtils.toGhdfsColumnType(col.type())))
								.toList();
		cinfoList.remove(geomCol.ordinal());
		cinfoList.add(geomCol.ordinal(), Pair.of("shape", GHDFSColumnType.COLUMN_TYPE_BINARY));
		gmeta.setColumnInfo(cinfoList, geomCol.ordinal());

		gmeta.setTotalCount(0); // 기록된 전체 레코드 수
		gmeta.setEnvelope(null); // 기록된 전체 레코드에 대한 Envelope

		FileSystem fs = FileSystem.get(conf);
		GHDFSWriter writer = new GHDFSWriter(fs, gmeta);
		writer.create(m_force);
		
		return writer;
	}

	public static StoreAsGHdfs fromProto(StoreAsGHdfsProto proto) {
		GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeomColInfo());
		return new StoreAsGHdfs(proto.getLayerName(), gcInfo, proto.getForce());
	}

	@Override
	public StoreAsGHdfsProto toProto() {
		return StoreAsGHdfsProto.newBuilder()
									.setLayerName(m_layerName)
									.setGeomColInfo(m_gcInfo.toProto())
									.setForce(m_force)
									.build();
	}
}
