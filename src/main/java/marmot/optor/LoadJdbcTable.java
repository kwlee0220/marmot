package marmot.optor;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import utils.Utilities;
import utils.jdbc.JdbcProcessor;
import utils.stream.KVFStream;

import marmot.Column;
import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.exec.MarmotExecutionException;
import marmot.externio.jdbc.GeometryFormat;
import marmot.externio.jdbc.JdbcRecordAdaptor;
import marmot.externio.jdbc.JdbcRecordSet;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.jdbc.JdbcInputFormat;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.plan.JdbcConnectOptions;
import marmot.plan.LoadJdbcTableOptions;
import marmot.proto.optor.LoadJdbcTableProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadJdbcTable extends AbstractRecordSetLoader implements MapReduceableRecordSetLoader,
																PBSerializable<LoadJdbcTableProto> {
	private final String m_tableName;
	private final JdbcConnectOptions m_jdbcOpts;
	private final LoadJdbcTableOptions m_options;
	private final JdbcProcessor m_jdbc;
	
	private long m_rowCount = -1;
	
	private LoadJdbcTable(String tableName, JdbcConnectOptions jdbcOpts,
							LoadJdbcTableOptions opts) {
		Utilities.checkNotNullArgument(jdbcOpts, "JdbcConnectionOptions is null");
		Utilities.checkNotNullArgument(tableName, "target table name is null");
		Utilities.checkNotNullArgument(opts, "LoadJdbcTableOptions is null");

		m_tableName = tableName;
		m_jdbcOpts = jdbcOpts;
		m_options = opts;
		
		m_jdbc = new JdbcProcessor(jdbcOpts.jdbcUrl(), jdbcOpts.user(),
									jdbcOpts.passwd(), jdbcOpts.driverClassName());
	}
	
	public String getTableName() {
		return m_tableName;
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		RecordSchema outSchema = m_options.selectExpr()
											.map(this::buildRecordSchema)
											.getOrElse(this::buildRecordSchema);
		setInitialized(marmot, outSchema);
	}
	
	@Override
	public void configure(Job job) {
		checkInitialized();
		
		Configuration conf = job.getConfiguration();
		JdbcProcessor jdbc = new JdbcProcessor(m_jdbcOpts.jdbcUrl(), m_jdbcOpts.user(),
												m_jdbcOpts.passwd(), m_jdbcOpts.driverClassName());
		JdbcInputFormat.setJdbcString(conf, jdbc);
		JdbcInputFormat.setJdbcInputTableName(conf, m_tableName);
		m_options.selectExpr().ifPresent(expr -> JdbcInputFormat.setJdbcInputColumnsExpr(conf, expr));
		m_options.mapperCount().ifPresent(count -> JdbcInputFormat.setJdbcInputMapperCount(conf, count));
		JdbcInputFormat.setJdbcInputRowCount(conf, m_rowCount);
		JdbcInputFormat.setJdbcInputSchema(conf, getRecordSchema());
		job.setInputFormatClass(JdbcInputFormat.class);
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		try {
			m_rowCount = m_jdbc.rowCount(m_tableName);
			if ( m_options.mapperCount().isPresent() ) {
				return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
			}
			
			if ( m_rowCount <= JdbcInputFormat.DEFAULT_MAP_SIZE ) {
				return PlanMRExecutionMode.LOCALLY_EXECUTABLE;
			}
			else if ( m_rowCount < 1.7 * JdbcInputFormat.DEFAULT_MAP_SIZE ) {
				return PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
			}
			
			return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		catch ( SQLException e ) {
			throw new MarmotExecutionException(e);
		}
	}

	@Override
	public RecordSet load() {
		checkInitialized();
		
		try {
			StringBuilder sqlBuilder = new StringBuilder("select ");
			String colsExpr = m_options.selectExpr()
										.getOrElse(() -> getRecordSchema().streamColumns()
																.map(Column::name)
																.join(","));
			sqlBuilder.append(colsExpr);
			sqlBuilder.append(" from ").append(m_tableName);
			ResultSet rs = m_jdbc.executeQuery(sqlBuilder.toString(), true);

			JdbcRecordAdaptor adaptor = JdbcRecordAdaptor.create(m_jdbc, getRecordSchema(),
																	GeometryFormat.WKB);
			return new JdbcRecordSet(adaptor, rs);
			
		}
		catch ( SQLException e ) {
			throw new RecordSetException("fails to create " + getClass(), e);
		}
	}

	@Override
	public String getInputString() {
		return String.format("url=%s,user=%s,driver=%s,table=%s",
							m_jdbcOpts.jdbcUrl(), m_jdbcOpts.user(),
							m_jdbcOpts.driverClassName(), m_tableName);
	}
	
	@Override
	public String toString() {
		return String.format("load_jdbc_table[url=%s,user=%s,driver=%s,table=%s]",
							m_jdbcOpts.jdbcUrl(), m_jdbcOpts.user(),
							m_jdbcOpts.driverClassName(), m_tableName);
	}

	public static LoadJdbcTable fromProto(LoadJdbcTableProto proto) {
		JdbcConnectOptions jdbcOpts = JdbcConnectOptions.fromProto(proto.getJdbcOptions());
		LoadJdbcTableOptions opts = LoadJdbcTableOptions.fromProto(proto.getOptions());
		return new LoadJdbcTable(proto.getTableName(), jdbcOpts, opts);
	}

	@Override
	public LoadJdbcTableProto toProto() {
		return LoadJdbcTableProto.newBuilder()
								.setTableName(m_tableName)
								.setJdbcOptions(m_jdbcOpts.toProto())
								.setOptions(m_options.toProto())
								.build();
	}
	
	private RecordSchema buildRecordSchema(String selectExpr) {
		try {
			String sql = String.format("select %s from %s limit 1", selectExpr, m_tableName);
			ResultSet rs = m_jdbc.executeQuery(sql, true);
			ResultSetMetaData meta = rs.getMetaData();
			
			RecordSchema.Builder builder = RecordSchema.builder();
			for ( int i =1; i <= meta.getColumnCount(); ++i ) {
				DataType type = fromSqlType(meta.getColumnType(i), meta.getColumnName(i));
				builder.addColumn(meta.getColumnLabel(i), type);
			}
			return builder.build();
		}
		catch ( Exception e ) {
			throw new MarmotExecutionException(e);
		}
	}
	
	private RecordSchema buildRecordSchema() {
		try {
			return KVFStream.from(m_jdbc.getColumns(m_tableName))
							.mapValue((k,v) -> fromSqlType(v.type(), v.typeName()))
							.fold(RecordSchema.builder(),
										(b,kv) -> b.addColumn(kv.key(), kv.value()))
							.build();
		}
		catch ( Exception e ) {
			throw new MarmotExecutionException(e);
		}
	}
	
	private DataType fromSqlType(int type, String typeName) {
		switch ( type ) {
			case Types.VARCHAR:
				return DataType.STRING;
			case Types.INTEGER:
				return DataType.INT;
			case Types.DOUBLE:
			case Types.NUMERIC:
				return DataType.DOUBLE;
			case Types.FLOAT:
			case Types.REAL:
				return DataType.FLOAT;
			case Types.BINARY:
			case Types.VARBINARY:
				return DataType.BINARY;
			case Types.BIGINT:
				return DataType.LONG;
			case Types.BOOLEAN:
				return DataType.BOOLEAN;
			case Types.SMALLINT:
				return DataType.SHORT;
			case Types.TINYINT:
				return DataType.BYTE;
			default:
				throw new IllegalArgumentException("unsupported SqlTypes: type=" + typeName
													+ ", code=" + type);
		}
	}
}
