package marmot.mapreduce;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.MarmotInternalException;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.RecordWritable;
import marmot.io.mapreduce.seqfile.MarmotFileOutputFormat;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.MapOutputWriterProto;
import marmot.protobuf.PBUtils;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import utils.StopWatch;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapOutputWriter extends AbstractRecordSetConsumer
								implements MapReduceJobConfigurer, MarmotMRContextAware,
											ProgressReportable,
											PBSerializable<MapOutputWriterProto> {
	private static final NullWritable NULL_KEY = NullWritable.get();
	
	private final FOption<MarmotMapOutputKeyColumns> m_mapOutputKeyCols;
	private MarmotMapOutputKey m_key;
	@SuppressWarnings("rawtypes")
	private TaskInputOutputContext m_context;
	private long m_count;
	private volatile RecordSet m_inputRSet;
	
	MapOutputWriter(FOption<MarmotMapOutputKeyColumns> mokCols) {
		m_mapOutputKeyCols = mokCols;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_key = m_mapOutputKeyCols.map(mokCols -> mokCols.create(inputSchema))
									.getOrNull();
		
		super.initialize(marmot, inputSchema);
	}

	@Override
	public void configure(Job job) {
		job.setOutputFormatClass(LazyOutputFormat.class);
		LazyOutputFormat.setOutputFormatClass(job, MarmotFileOutputFormat.class);
		
		job.setMapOutputValueClass(RecordWritable.class);
	}

	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		m_context = context.getHadoopContext();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		Preconditions.checkState(m_context != null, "not in a Map-Reduce context");
		
		RecordWritable value = RecordWritable.from(rset.getRecordSchema());
		
		m_inputRSet = rset;
		m_count = 0;
		Record record = DefaultRecord.of(rset.getRecordSchema());

		try {
			while ( rset.next(record) ) {
				Writable key = ( m_key != null ) ? m_key.load(record) : NULL_KEY;
				value.loadFrom(record);
				m_context.write(key, value);
				
				++m_count;
			}
		}
		catch ( Exception e ) {
			Map<String,Object> values = record.toMap();
			Map<String,Object> reduced = record.getRecordSchema().streamColumns()
												.filter(c -> !c.type().isGeometryType())
												.toMap(c -> c.name(), c -> values.get(c.name()));
			
			String msg = String.format("fails to write mapper output: record=%s, cause=%s",
										reduced, "" + e);
			getLogger().error(msg);
			throw new MarmotInternalException(msg);
		}
		finally {
//			m_inputRSet = null;
			rset.closeQuietly();
		}
	}

	@Override
	public void reportProgress(Logger logger, StopWatch elapsed) {
		RecordSet rset = m_inputRSet;
		if ( rset != null && rset instanceof ProgressReportable ) {
			((ProgressReportable)rset).reportProgress(logger, elapsed);
		}
		
		long velo = Math.round(m_count / elapsed.getElapsedInFloatingSeconds());
		logger.info("report: {}: count={} elapsed={} velo={}/s",
							this, m_count, elapsed.getElapsedMillisString(), velo);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(getClass().getSimpleName() + ": ");
		m_mapOutputKeyCols.ifPresent(keyCols -> builder.append("keys={" + keyCols + "}, "));
		if ( isInitialized() ) {
			builder.append("schema={").append(getInputRecordSchema()).append("}");
		}
		else {
			builder.append("schema=unknown");
		}
		
		return builder.toString();
	}

	public static MapOutputWriter fromProto(MapOutputWriterProto proto) {
		FOption<MarmotMapOutputKeyColumns> keyCols
							= PBUtils.<String>getOptionField(proto, "map_output_key_cols")
									.map(MarmotMapOutputKeyColumns::fromString);
		
		return new MapOutputWriter(keyCols);
	}

	@Override
	public MapOutputWriterProto toProto() {
		MapOutputWriterProto.Builder builder = MapOutputWriterProto.newBuilder();
		m_mapOutputKeyCols.map(MarmotMapOutputKeyColumns::toString)
							.ifPresent(builder::setMapOutputKeyCols);
		return builder.build();
	}
}
