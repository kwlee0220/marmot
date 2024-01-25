package marmot.mapreduce.input.jdbc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import marmot.Column;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.externio.jdbc.GeometryFormat;
import utils.jdbc.JdbcProcessor;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class JdbcRecordReader extends RecordReader<LongWritable, Record>
									implements AutoCloseable {
	private JdbcTableSplit m_split;
	private RecordSchema m_schema;
	
	private RecordSet m_rset;
	private long m_rowIndex;
	private Record m_next;

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
		throws IOException, InterruptedException {
		m_split = (JdbcTableSplit)split;
		
		Configuration conf = context.getConfiguration();
		
		JdbcProcessor jdbc = JdbcInputFormat.getJdbcString(conf);
		m_schema = JdbcInputFormat.getJdbcInputSchema(conf);

		String tblName = JdbcInputFormat.getJdbcInputTableName(conf);
		String colExpr = JdbcInputFormat.getJdbcInputColumnsExpr(conf)
							.getOrElse(() -> m_schema.streamColumns()
												.map(JdbcRecordReader::toSqlColumnExpr)
												.join(","));

		String sql = String.format("select %s from %s", colExpr, tblName);
		m_rset = RecordSet.concat(m_schema, new BlockRecordSetSupplier(m_schema, jdbc, GeometryFormat.WKB,
															sql, m_split.m_start, m_split.m_length));
		m_rowIndex = m_split.m_start -1;
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		m_next = m_rset.nextCopy();
		if ( m_next != null ) {
			++m_rowIndex;
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return new LongWritable(m_rowIndex);
	}

	@Override
	public Record getCurrentValue() throws IOException, InterruptedException {
		return m_next;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (float)((double)(m_rowIndex - m_split.m_start) / m_split.m_length);
	}

	@Override
	public void close() throws IOException {
		m_rset.closeQuietly();
	}
	
	private static String toSqlColumnExpr(Column col) {
		return col.type().isGeometryType()
				? "ST_AsBinary(" + col.name() + ")"
				: col.name();
	}
}