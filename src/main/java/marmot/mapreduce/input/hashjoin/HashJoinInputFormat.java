package marmot.mapreduce.input.hashjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import marmot.Record;
import marmot.RecordSchema;
import marmot.io.RecordWritable;
import marmot.io.serializer.MarmotSerializable;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.join.MapReduceHashJoin;
import marmot.support.DefaultRecord;
import marmot.type.DataType;
import utils.CSV;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HashJoinInputFormat extends SequenceFileInputFormat<NullWritable, RecordWritable> {
	public static class Parameters implements MarmotSerializable {
		private final RecordSchema m_schema;
		private final String m_joinCols;
		private final boolean m_useCache;
		
		public Parameters(RecordSchema schema, String joinCols, boolean useCache) {
			m_schema = schema;
			m_joinCols = joinCols;
			m_useCache = useCache;
		}
		
		public static Parameters deserialize(DataInput input) {
			RecordSchema schema = MarmotSerializers.RECORD_SCHEMA.deserialize(input);
			String joinCols = MarmotSerializers.readString(input);
			boolean useCache = MarmotSerializers.readBoolean(input);
			
			return new Parameters(schema, joinCols, useCache);
		}

		@Override
		public void serialize(DataOutput output) {
			MarmotSerializers.RECORD_SCHEMA.serialize(m_schema, output);
			MarmotSerializers.writeString(m_joinCols, output);
			MarmotSerializers.writeBoolean(m_useCache, output);
		}
		
		@Override
		public String toString() {
			String cacheStr = m_useCache ? ", cache" : ", no-cache";
			return String.format("%s[jcols={%s}]%s",
								getClass().getSimpleName(), m_joinCols, cacheStr);
		}
	}
	
	public static RecordSchema calcOutputRecordSchema(Parameters params) {
		RecordSchema inputSchema = params.m_schema;

		RecordSchema.Builder builder = MapReduceHashJoin.SCHEMA_PREFIX.toBuilder();
		String[] jColNames = CSV.parseCsv(params.m_joinCols).toArray(String.class);
		for ( int i = 0; i < jColNames.length; ++i ) {
			String jColName = String.format("jc%02d", i);
			DataType jColType = inputSchema.getColumn(jColNames[i]).type();
			builder.addColumn(jColName, jColType);
		}
		return builder.build();
	}
	
	public static abstract class AbstractReader extends RecordReader<NullWritable, RecordWritable>
												implements MarmotRecordReader {
		private final RecordReader<NullWritable, RecordWritable> m_baseReader;
		private final int m_datasetIdx;
		private boolean m_cacheable;
		private int[] m_joinColIdxes;
		private Record m_input;
		private RecordWritable m_output;
		
		AbstractReader(int datasetIdx) {
			m_datasetIdx = datasetIdx;
			m_baseReader = new SequenceFileRecordReader<>();
		}
		
		public RecordSchema getRecordSchema() {
			return m_output.getRecordSchema();
		}

		protected void initialize(InputSplit split, TaskAttemptContext context,
									Parameters params) throws IOException, InterruptedException {
			m_baseReader.initialize(split, context);
			
			m_cacheable = params.m_useCache;
			
			RecordSchema inputSchema = params.m_schema;
			m_input = DefaultRecord.of(inputSchema);
			m_output = RecordWritable.from(calcOutputRecordSchema(params));
			
			m_joinColIdxes = CSV.parseCsv(params.m_joinCols, ',', '\\')
									.mapToInt(colName -> inputSchema.getColumn(colName).ordinal())
									.toArray();
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return m_baseReader.nextKeyValue();
		}

		@Override
		public NullWritable getCurrentKey() throws IOException, InterruptedException {
			return NullWritable.get();
		}

		@Override
		public RecordWritable getCurrentValue() throws IOException, InterruptedException {
			RecordWritable value = m_baseReader.getCurrentValue();
			value.storeTo(m_input);
			
			return toOutput(m_input);
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return m_baseReader.getProgress();
		}

		@Override
		public void close() throws IOException {
			m_baseReader.close();
		}

		private RecordWritable toOutput(Record input) {
			m_output.set(MapReduceHashJoin.TAG_IDX_DATASET_IDX, (byte)m_datasetIdx);
			m_output.set(MapReduceHashJoin.TAG_IDX_CACHEABLE, m_cacheable ? (byte)1 : (byte)0);
			m_output.set(MapReduceHashJoin.TAG_IDX_DECODED, RecordWritable.from(input).toBytes());
			
			int jcIdx = 3;
			for ( int colIdx: m_joinColIdxes ) {
				m_output.set(jcIdx++, input.get(colIdx));
			}
			
			return m_output;
		}
	}
}
