package marmot.optor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.mapreduce.MapReduceJobConfigurer;
import marmot.mapreduce.MapReduceTerminal;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.plan.JdbcConnectOptions;
import marmot.proto.optor.StoreIntoJdbcTableProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.LocalDateTimes;
import utils.LocalDates;
import utils.LocalTimes;
import utils.Utilities;
import utils.func.FOption;
import utils.jdbc.JdbcProcessor;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreIntoJdbcTable extends AbstractRecordSetConsumer
								implements MapReduceTerminal, MapReduceJobConfigurer,
											PBSerializable<StoreIntoJdbcTableProto> {
	private static final int DEFAULT_BATCH_SIZE = 64;
	private static final int DISPLAY_GAP = 100000;

	private final String m_tableName;
	private final JdbcConnectOptions m_jdbcOpts;
	private final FOption<String> m_valuesExpr;
	private int m_batchSize = DEFAULT_BATCH_SIZE;
	
	public StoreIntoJdbcTable(String tableName, JdbcConnectOptions jdbcOpts,
								FOption<String> valuesExpr) {
		Utilities.checkNotNullArgument(tableName, "tableName is null");
		Utilities.checkNotNullArgument(tableName, "target table name is null");
		Utilities.checkNotNullArgument(valuesExpr, "valuesExpr is null");

		m_tableName = tableName;
		m_jdbcOpts = jdbcOpts;
		m_valuesExpr = valuesExpr;
		
		setLogger(LoggerFactory.getLogger(StoreIntoJdbcTable.class));
	}
	
	public StoreIntoJdbcTable batchSize(int size) {
		m_batchSize = size;
		return this;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
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
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		String valuesExpr = m_valuesExpr.getOrElse(() -> {
			StringBuilder valuesBuilder = new StringBuilder();
			valuesBuilder.append(rset.getRecordSchema()
						.streamColumns()
						.map(Column::name)
						.join(",", "(", ")"));
			valuesBuilder.append(" values ");
			valuesBuilder.append(IntStream.range(0, rset.getRecordSchema().getColumnCount())
						.mapToObj(idx -> "?")
						.collect(Collectors.joining(",", "(", ")")));
			return valuesBuilder.toString();
		});
		String insertStmtStr = String.format("insert into %s %s", m_tableName, valuesExpr);

		JdbcProcessor jdbc = new JdbcProcessor(m_jdbcOpts.jdbcUrl(), m_jdbcOpts.user(),
												m_jdbcOpts.passwd(), m_jdbcOpts.driverClassName());

		AtomicInteger count = new AtomicInteger(0);
		try ( Connection conn = jdbc.connect() ) {
			PreparedStatement pstmt = conn.prepareStatement(insertStmtStr);
			getLogger().info("connected: {}", this);
			
			rset.forEachCopy(record -> {
				try {
					fillStatement(jdbc, pstmt, record);
					pstmt.addBatch();
					
					count.incrementAndGet();
					if ( count.get() % m_batchSize == 0 ) {
						pstmt.executeBatch();

						getLogger().debug("inserted: {} records", count);
					}

					if ( count.get() % DISPLAY_GAP == 0 ) {
						getLogger().info("inserted: {} records", count);
					}
				}
				catch ( SQLException e ) {
					throw new RecordSetException(String.format("fails to store output: jdbc=%s, table=%s",
													m_jdbcOpts.jdbcUrl(), m_tableName), e);
				}
			});
			pstmt.executeBatch();
		}
		catch ( Exception e ) {
			System.out.println("count=" + count.get());
			throw new RecordSetException(e);
		}
		getLogger().info("inserted: {} records", count.get());
	}
	
	@Override
	public String toString() {
		return String.format("store_into_jdbc[%s,url=%s,user=%s]",
								m_tableName, m_jdbcOpts.jdbcUrl(), m_jdbcOpts.user());
	}
	
	private void fillStatement(JdbcProcessor jdbc, PreparedStatement pstmt,
								Record record) throws SQLException {
		RecordSchema schema = record.getRecordSchema();
		Object[] values = record.getAll();

		String str;
		for ( int i =0; i < schema.getColumnCount(); ++i ) {
			Column col = schema.getColumnAt(i);
			switch ( col.type().getTypeCode() ) {
				case STRING:
					// PostgreSQL의 경우 문자열에 '0x00'가 포함되는 경우
					// 오류를 발생시키기 때문에, 삽입전에 제거시킨다.
					str = (String)values[i];
					if ( str != null ) {
						str = str.replaceAll("\\x00","");
					}
					pstmt.setString(i+1, str);
					break;
				case INT:
					pstmt.setInt(i+1, (Integer)values[i]);
					break;
				case LONG:
					pstmt.setLong(i+1, (Long)values[i]);
					break;
				case SHORT:
					pstmt.setShort(i+1, (Short)values[i]);
					break;
				case DOUBLE:
					pstmt.setDouble(i+1, (Double)values[i]);
					break;
				case FLOAT:
					pstmt.setFloat(i+1, (Float)values[i]);
					break;
				case BINARY:
					pstmt.setBytes(i+1, (byte[])values[i]);
					break;
				case DATETIME:
					pstmt.setLong(i+1, LocalDateTimes.toUtcMillis((LocalDateTime)values[i]));
					break;
				case DATE:
					pstmt.setLong(i+1, LocalDates.toUtcMillis((LocalDate)values[i]));
					break;
				case TIME:
					pstmt.setString(i+1, LocalTimes.toString((LocalTime)values[i]));
					break;
				case BOOLEAN:
					pstmt.setBoolean(i+1, (Boolean)values[i]);
					break;
				default:
					throw new RecordSetException("unexpected DataType: " + col);
			}
		}
	}

	public static StoreIntoJdbcTable fromProto(StoreIntoJdbcTableProto proto) {
		JdbcConnectOptions jdbcOpts = JdbcConnectOptions.fromProto(proto.getJdbcOptions());
		FOption<String> valuesExpr = PBUtils.getOptionField(proto, "values_expr");
		return new StoreIntoJdbcTable(proto.getTableName(), jdbcOpts, valuesExpr);
	}

	@Override
	public StoreIntoJdbcTableProto toProto() {
		StoreIntoJdbcTableProto.Builder builder = StoreIntoJdbcTableProto.newBuilder()
															.setTableName(m_tableName)
															.setJdbcOptions(m_jdbcOpts.toProto());
		m_valuesExpr.ifPresent(expr ->  builder.setValuesExpr(expr));
		return builder.build();
	}
}
