package marmot.mapreduce.input.jdbc;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import marmot.Record;
import marmot.RecordSchema;
import marmot.exec.MarmotExecutionException;
import utils.CSV;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class JdbcInputFormat extends InputFormat<LongWritable, Record> {
	public static final long DEFAULT_MAP_SIZE = 500_000;
	private static final String PROP_JDBC_INPUT_JDBC = "marmot.optor.load_jdbc.jdbc";
	private static final String PROP_JDBC_INPUT_SCHEMA = "marmot.optor.load_jdbc.record_schema";
	private static final String PROP_JDBC_INPUT_TBLNAME = "marmot.optor.load_jdbc.table_name";
	private static final String PROP_JDBC_INPUT_COLS_EXPR= "marmot.optor.load_jdbc.cols_expr";
	private static final String PROP_JDBC_INPUT_MAPPER_COUNT = "marmot.optor.load_jdbc.mapper_count";
	private static final String PROP_JDBC_INPUT_ROW_COUNT = "marmot.optor.load_jdbc.row_count";

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		try {
			long rowCount = getJdbcInputRowCount(conf);
			int mapperCount = getJdbcInputMapperCount(conf).getOrElse(-1);
			if ( mapperCount == -1 ) {
				mapperCount = (int)Math.ceil(rowCount / (double)DEFAULT_MAP_SIZE);
			}
			long splitSize = Math.round(rowCount / (double)mapperCount);
			
			List<InputSplit> splits = Lists.newArrayList();
			for ( int i =0; i < mapperCount-1; ++i ) {
				splits.add(new JdbcTableSplit(i*splitSize, splitSize));
			}
			splits.add(new JdbcTableSplit((mapperCount-1)*splitSize, -splitSize));
			
			return splits;
		}
		catch ( Exception e ) {
			throw new MarmotExecutionException(e);
		}
	}

	@Override
	public RecordReader<LongWritable, Record> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new JdbcRecordReader();
	}

	public static final RecordSchema getJdbcInputSchema(Configuration conf) {
		String str = conf.get(PROP_JDBC_INPUT_SCHEMA);
		if ( str == null ) {
			throw new MarmotExecutionException("Hadoop configuration property not found: prop="
											+ PROP_JDBC_INPUT_SCHEMA);
		}
		
		return RecordSchema.parse(str);
	}
	public static final void setJdbcInputSchema(Configuration conf, RecordSchema schema) {
		conf.set(PROP_JDBC_INPUT_SCHEMA, schema.toString());
	}
	
	public static final String getJdbcInputTableName(Configuration conf) {
		String str = conf.get(PROP_JDBC_INPUT_TBLNAME);
		if ( str == null ) {
			throw new MarmotExecutionException("Hadoop configuration property not found: prop="
											+ PROP_JDBC_INPUT_TBLNAME);
		}
		
		return str;
	}
	public static final void setJdbcInputTableName(Configuration conf, String tblName) {
		conf.set(PROP_JDBC_INPUT_TBLNAME, tblName);
	}
	
	public static final FOption<String> getJdbcInputColumnsExpr(Configuration conf) {
		return FOption.ofNullable(conf.get(PROP_JDBC_INPUT_COLS_EXPR));
	}
	public static final void setJdbcInputColumnsExpr(Configuration conf, String colsExpr) {
		conf.set(PROP_JDBC_INPUT_COLS_EXPR, colsExpr);
	}
	
	public static final JdbcProcessor getJdbcString(Configuration conf) {
		String str = conf.get(PROP_JDBC_INPUT_JDBC);
		if ( str == null ) {
			throw new MarmotExecutionException("Hadoop configuration property not found: prop="
											+ PROP_JDBC_INPUT_JDBC);
		}
		
		return fromJdbcProcessorString(str);
	}
	public static final void setJdbcString(Configuration conf, JdbcProcessor jdbc) {
		conf.set(PROP_JDBC_INPUT_JDBC, toJdbcProcessorString(jdbc));
	}
	
	public static final String toJdbcProcessorString(JdbcProcessor jdbc) {
		return CSV.get().withDelimiter(',').withQuote('\'')
					.toString(jdbc.getJdbcUrl(), jdbc.getUser(), jdbc.getPassword(),
								jdbc.getDriverClassName());
	}
	public static final JdbcProcessor fromJdbcProcessorString(String str) {
		String[] parts = CSV.parseCsv(str).toArray(String.class);
		return new JdbcProcessor(parts[0], parts[1], parts[2], parts[3]);
	}
	
	public static final FOption<Integer> getJdbcInputMapperCount(Configuration conf) {
		return FOption.ofNullable(conf.get(PROP_JDBC_INPUT_MAPPER_COUNT))
						.map(Integer::parseInt);
	}
	public static final void setJdbcInputMapperCount(Configuration conf, int count) {
		conf.set(PROP_JDBC_INPUT_MAPPER_COUNT, ""+count);
	}
	
	public static final long getJdbcInputRowCount(Configuration conf) {
		return Long.parseLong(conf.get(PROP_JDBC_INPUT_ROW_COUNT));
	}
	public static final void setJdbcInputRowCount(Configuration conf, long count) {
		conf.set(PROP_JDBC_INPUT_ROW_COUNT, ""+count);
	}
}
