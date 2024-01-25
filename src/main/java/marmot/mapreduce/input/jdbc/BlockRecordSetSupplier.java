package marmot.mapreduce.input.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.externio.jdbc.GeometryFormat;
import marmot.externio.jdbc.JdbcRecordAdaptor;
import marmot.externio.jdbc.JdbcRecordSet;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class BlockRecordSetSupplier implements FStream<RecordSet> {
	private static final int BLOCK_SIZE = 8 * 1024;

	private final JdbcProcessor m_jdbc;
	private final JdbcRecordAdaptor m_adaptor;
	private final String m_sql;
	private final long m_end;
	private long m_blockStart;
	private long m_blockEnd;
	
	BlockRecordSetSupplier(RecordSchema schema, JdbcProcessor jdbc, GeometryFormat geomFormat,
							String sql, long start, long length) {
		m_jdbc = jdbc;
		m_adaptor = JdbcRecordAdaptor.create(jdbc, schema, geomFormat);
		m_sql = sql;
		m_end = (length >= 0) ? start + length : Long.MAX_VALUE;
		m_blockStart = start;
		m_blockEnd = start;
	}

	@Override
	public void close() throws Exception { }

	@Override
	public FOption<RecordSet> next() {
		try {
			m_blockStart = m_blockEnd;
			m_blockEnd = Math.min(m_blockStart + BLOCK_SIZE, m_end);
			
			String blockSql = String.format("%s offset %d limit %d", m_sql,
											m_blockStart, m_blockEnd-m_blockStart);
			ResultSet rs = m_jdbc.executeQuery(blockSql);
			return new JdbcRecordSet(m_adaptor, rs).asNonEmpty();
		}
		catch ( SQLException e ) {
			throw new RecordSetException(e);
		}
	}
}