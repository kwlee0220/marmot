package marmot.optor;

import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.DataSetPartitionInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.mapreduce.HdfsWriterTerminal;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContextAware;
import marmot.mapreduce.StageWorkspaceAware;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.StoreDataSetPartitionProto;
import marmot.rset.ReducerRecordSet;
import marmot.support.PBSerializable;
import utils.UnitUtils;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreDataSetPartition extends AbstractRecordSetFunction
								implements HdfsWriterTerminal, MapReduceJobConfigurer,
											StageWorkspaceAware, MarmotMRContextAware,
											PBSerializable<StoreDataSetPartitionProto> {
	private final Path m_filePath;
	private final StoreDataSetOptions m_opts;

	private Path m_partitionPath;
	private Path m_mrOutputPath;
	
	public StoreDataSetPartition(Path filePath, StoreDataSetOptions opts) {
		Utilities.checkNotNullArgument(filePath, "filePath is null");
		Utilities.checkNotNullArgument(opts, "StoreDataSetOptions is null");
		
		m_filePath = filePath;
		m_opts = opts;
		m_partitionPath = m_filePath;
		
		setLogger(LoggerFactory.getLogger(StoreDataSetPartition.class));
	}
	
	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		m_partitionPath = new Path(m_filePath, String.format("%05d", context.getTaskOrdinal()));
	}

	@Override
	public void setStageWorkspace(Path workspaceDir) {
		m_mrOutputPath = new Path(workspaceDir, UUID.randomUUID().toString());
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, DataSetPartitionInfo.SCHEMA);
	}

	@Override
	public Path getOutputPath() {
		Preconditions.checkState(m_mrOutputPath != null, "File path has not been set");
		
		return m_mrOutputPath;
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();
		
		RecordSchema inputSchema = getInputRecordSchema();
		MarmotFileOutputFormat.setRecordSchema(job.getConfiguration(), inputSchema);
		
		m_opts.metaData().ifPresent(meta -> {
			for ( Map.Entry<String, String> prop: meta.entrySet() ) {
				job.getConfiguration().set(prop.getKey(), prop.getValue());
			}
		});

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputFormatClass(NullOutputFormat.class);
		job.setOutputValueClass(NullWritable.class);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		HdfsPath path = HdfsPath.of(getMarmotCore().getHadoopConfiguration(), m_partitionPath);
		GeometryColumnInfo gcInfo = m_opts.geometryColumnInfo().getOrNull();
		MarmotFileWriteOptions opts = m_opts.writeOptions();
		
		return new ReducerRecordSet(input) {
			@Override
			public RecordSchema getRecordSchema() {
				return DataSetPartitionInfo.SCHEMA;
			}

			@Override
			protected Record reduce() {
				return MarmotSequenceFile.store(path, input, gcInfo, opts)
						.call().toRecord();
			}
			
			@Override
			public String toString() {
				return StoreDataSetPartition.this.toString();
			}
		};
	}
	
	@Override
	public String toString() {
		String blkStr = (m_marmot != null)
						? m_opts.blockSize().orElse(m_marmot.getDefaultMarmotBlockSize())
								.map(UnitUtils::toByteSizeString)
								.get()
						: m_opts.blockSize().map(UnitUtils::toByteSizeString)
								.getOrElse("default");
		String codecStr = m_opts.compressionCodecName()
								.map(codec -> ", compress=" + codec)
								.getOrElse("");
		return String.format("%s: path=%s%s, block=%s", getClass().getSimpleName(), m_partitionPath,
								codecStr, blkStr);
	}

	public static StoreDataSetPartition fromProto(StoreDataSetPartitionProto proto) {
		return new StoreDataSetPartition(new Path(proto.getPath()),
										StoreDataSetOptions.fromProto(proto.getOptions()));
	}

	@Override
	public StoreDataSetPartitionProto toProto() {
		return StoreDataSetPartitionProto.newBuilder()
										.setPath(m_filePath.toString())
										.setOptions(m_opts.toProto())
										.build();
	}
}
