package marmot.mapreduce.input.spatialjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import marmot.RecordSchema;
import marmot.dataset.DataSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.dataset.IndexNotFoundException;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.io.HdfsPath;
import marmot.io.geo.index.GlobalIndex;
import marmot.io.serializer.MarmotSerializer;
import marmot.io.serializer.MarmotSerializers;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ClusterAccessInfo {
	final String m_clusterDir;
	final int m_geomColIdx;
	final String m_srid;
	final RecordSchema m_schema;
	
	public static ClusterAccessInfo from(DataSet ds) {
		GeometryColumnInfo gcInfo = ds.getGeometryColumnInfo();
		int geomColIdx = ds.getRecordSchema().getColumn(gcInfo.name()).ordinal();
		String srid = gcInfo.srid();
		SpatialIndexInfo idxInfo = ds.getSpatialIndexInfo()
										.getOrThrow(() -> IndexNotFoundException.fromDataSet(ds.getId()));
		return new ClusterAccessInfo(idxInfo.getHdfsFilePath(), geomColIdx, srid,
										ds.getRecordSchema());
	}
	
	ClusterAccessInfo(String clusterDir, int geomColIdx, String srid,
						RecordSchema schema) {
		m_clusterDir = clusterDir;
		m_geomColIdx = geomColIdx;
		m_srid = srid;
		m_schema = schema;
	}
	
	public GlobalIndex getIndexFile(Configuration conf)
		throws IllegalArgumentException, IOException {
		HdfsPath dirPath = HdfsPath.of(conf, new Path(m_clusterDir));
		return GlobalIndex.open(GlobalIndex.toGlobalIndexPath(dirPath));
	}

	public static Serializer getSerializer() {
		return s_serializer;
	}
	
	private static final Serializer s_serializer = new Serializer();
	private static class Serializer implements MarmotSerializer<ClusterAccessInfo> {
		@Override
		public void serialize(ClusterAccessInfo info, DataOutput output) {
			MarmotSerializers.writeString(info.m_clusterDir, output);
			MarmotSerializers.writeVInt(info.m_geomColIdx, output);
			MarmotSerializers.writeString(info.m_srid, output);
			MarmotSerializers.RECORD_SCHEMA.serialize(info.m_schema, output);
		}

		@Override
		public ClusterAccessInfo deserialize(DataInput input) {
			String clusterPath = MarmotSerializers.readString(input);
			int geomColIdx = MarmotSerializers.readVInt(input);
			String srid = MarmotSerializers.readString(input);
			RecordSchema schema = MarmotSerializers.RECORD_SCHEMA.deserialize(input);
			
			return new ClusterAccessInfo(clusterPath, geomColIdx, srid, schema);
		}
	}
}