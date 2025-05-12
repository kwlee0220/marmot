package marmot.mapreduce.input.fixed;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.exec.AnalysisStoreException;
import marmot.io.serializer.MarmotSerializers;

import utils.Tuple;
import utils.func.CheckedFunctionX;
import utils.func.FOption;
import utils.func.Try;
import utils.io.IOUtils;
import utils.jdbc.JdbcUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class InputSplitStore {
	private static final Logger s_logger = LoggerFactory.getLogger(InputSplitStore.class);
	
	private final Configuration m_conf;
	
	public static InputSplitStore open(Configuration conf) {
		return new InputSplitStore(conf);
	}
	
	private InputSplitStore(Configuration conf) {
		m_conf = conf;
	}
	
	public static InputSplitStore createStore(Configuration conf) throws SQLException {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(SQL_CREATE_INPUT_SPLITS);
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
		
		return open(conf);
	}
	
	public static void dropStore(Configuration conf) throws SQLException {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			final Statement stmt = conn.createStatement();
			Try.run(()->stmt.executeUpdate(SQL_DROP_INPUT_SPLITS));
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
	}

	public FOption<Tuple<Integer,InputSplit>> poll(String jobId) throws SQLException {
		Connection conn = null;
		try {
			conn = getConnection(m_conf);
			conn.setAutoCommit(false);

			PreparedStatement selectPstmt = conn.prepareStatement(SQL_GET_INPUT_SPLIT);
			while ( true ) {
				selectPstmt.setString(1, jobId);
				FOption<Tuple<Integer,InputSplit>> osplit
								= JdbcUtils.fstream(selectPstmt.executeQuery(), s_toInputSplit)
											.findFirst();
				
				if ( osplit.isPresent() ) {
					Tuple<Integer,InputSplit> tuple = osplit.getUnchecked();
					
					PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_INPUT_SPLIT);
					pstmt.setString(1, jobId);
					pstmt.setInt(2, tuple._1);
					int cnt = pstmt.executeUpdate();
					if ( cnt > 0 ) {
						conn.commit();
						return osplit;
					}
					
					s_logger.info("retrying polling next FileSplit");
				}
				else {
					return osplit;
				}
			}
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
	}
	
	public void insertInputSplit(String jobId, List<InputSplit> splits, int mapperCount)
		throws SQLException, IOException {
		try ( Connection conn = getConnection(m_conf) ) {
			conn.setAutoCommit(false);
			
			PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_INPUT_SPLIT);
			for ( int i =0; i < splits.size(); ++i ) {
				InputSplit split = (InputSplit)splits.get(i);
				
				pstmt.setString(1, jobId);
				pstmt.setInt(2, i);
				
				if ( !(split instanceof Writable) ) {
					throw new IllegalArgumentException("input split is not Writable: " + split);
				}
				pstmt.setString(3, split.getClass().getName());
				
				Writable w = (Writable)split;
				byte[] serialized = MarmotSerializers.toBytes(split, o -> w.write(o));
				pstmt.setBytes(4, serialized);
				
				pstmt.executeUpdate();
			}
			conn.commit();
		}
	}
	
	public void removeJob(String jobId) throws SQLException {
		try ( Connection conn = getConnection(m_conf) ) {
			PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_JOB);
			pstmt.setString(1, jobId);
			pstmt.executeUpdate();
		}
	}
	
	private static Connection getConnection(Configuration conf) {
		String jdbcUrl = conf.get("marmot.catalog.jdbc.url");
		if ( jdbcUrl == null ) {
			throw new AnalysisStoreException("fails to get JDBC url: name=marmot.catalog.jdbc.url");
		}
		String user = conf.get("marmot.catalog.jdbc.user");
		if ( user == null ) {
			throw new AnalysisStoreException("fails to get JDBC user: name=marmot.catalog.jdbc.user");
		}
		String passwd = conf.get("marmot.catalog.jdbc.passwd");
		if ( passwd == null ) {
			throw new AnalysisStoreException("fails to get JDBC user: name=marmot.catalog.jdbc.passwd");
		}
		String driverClassName = conf.get("marmot.catalog.jdbc.driver");
		if ( driverClassName == null ) {
			throw new AnalysisStoreException("fails to get JDBC driver class: name=marmot.catalog.jdbc.driver");
		}
		
		try {
			Class.forName(driverClassName);
			return DriverManager.getConnection(jdbcUrl, user, passwd);
		}
		catch ( Exception e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	private static final String SQL_CREATE_INPUT_SPLITS
		= "create table input_splits ("
		+ 	"job_id varchar not null,"
		+ 	"seqno integer not null,"
		+	"split_class varchar not null,"
		+	"split_params bytea not null"
		+ ")";
	
	private static final String SQL_DROP_INPUT_SPLITS
		= "drop table input_splits";

	private static final String SQL_GET_INPUT_SPLIT
		= "select seqno, split_class, split_params "
		+ "from input_splits where job_id=? limit 1";
	
	private static final String SQL_INSERT_INPUT_SPLIT
		= "insert into input_splits "
		+	"(job_id, seqno, split_class, split_params) "
		+	"values (?,?,?,?)";
	
	private static final String SQL_DELETE_INPUT_SPLIT
		= "delete from input_splits where job_id = ? and seqno = ?";
	private static final String SQL_DELETE_JOB
		= "delete from input_splits where job_id = ?";
	
	private static final CheckedFunctionX<ResultSet,Tuple<Integer,InputSplit>,SQLException> s_toInputSplit = rs -> {
		int seqno = rs.getInt(1);
		String className = rs.getString(2);
		
		try {
			Class<?> splitClass = Class.forName(className);
			if ( !Writable.class.isAssignableFrom(splitClass)
				|| !InputSplit.class.isAssignableFrom(splitClass) ) {
				throw new IllegalStateException("InputSplit class does not implement Writable&InputSplit: class=" + className);
			}
			
			Writable w = (Writable)splitClass.newInstance();
			byte[] params = rs.getBytes(3);
			return MarmotSerializers.fromBytes(params, in -> {
				w.readFields(in);
				return Tuple.of(seqno, (InputSplit)w);
			});
		}
		catch ( Exception e ) {
			throw new IllegalStateException("fails to read InputStream: seqno=" + seqno);
		}
	};
}
