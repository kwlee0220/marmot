package marmot.exec;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.InvalidProtocolBufferException;

import utils.CSV;
import utils.Utilities;
import utils.func.CheckedFunctionX;
import utils.func.Try;
import utils.io.IOUtils;
import utils.jdbc.JdbcException;
import utils.jdbc.JdbcUtils;
import utils.stream.FStream;
import utils.stream.KeyValueFStream;

import marmot.ExecutePlanOptions;
import marmot.Plan;
import marmot.analysis.system.SystemAnalysis;
import marmot.exec.MarmotAnalysis.Type;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotAnalysisStore {
	private final Configuration m_conf;
	
	public static MarmotAnalysisStore initialize(Configuration conf) {
		return new MarmotAnalysisStore(conf);
	}
	
	private MarmotAnalysisStore(Configuration conf) {
		m_conf = conf;
	}
	
	public static MarmotAnalysisStore createStore(Configuration conf) {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			Statement stmt = conn.createStatement();
			stmt.executeUpdate(SQL_CREATE_ANALYSIS);
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
		
		return initialize(conf);
	}
	
	public static void dropStore(Configuration conf) {
		Connection conn = null;
		try {
			conn = getConnection(conf);
			
			final Statement stmt = conn.createStatement();
			Try.run(()->stmt.executeUpdate("drop table analyses"));
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
		finally {
			IOUtils.closeQuietly(conn);
		}
	}

	public MarmotAnalysis get(String id) {
		MarmotAnalysis anal = find(id);
		if ( anal == null ) {
			throw new AnalysisNotFoundException(id);
		}
		
		return anal;
	}

	public MarmotAnalysis find(String id) {
		try ( Connection conn = getConnection(m_conf); ) {
			return findInGuard(conn, id);
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	public List<MarmotAnalysis> getAll() {
		try ( Connection conn = getConnection(m_conf) ) {
			
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
		
		try ( Connection conn = getConnection(m_conf);
			PreparedStatement pstmt = conn.prepareStatement(SQL_GET_ANALYSIS_ALL); ) {
			return JdbcUtils.fstream(pstmt.executeQuery(), s_toAnalysis)
							.toList();
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}
	
	public List<MarmotAnalysis> getAllOfType(Type type) {
		try ( Connection conn = getConnection(m_conf) ) {
			return getAllOfTypeInGuard(conn, type);
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
	}

	public void add(MarmotAnalysis analysis, boolean force) {
		Utilities.checkNotNullArgument(analysis, "MarmotAnalysis should not be null.");

		String id = analysis.getId();
		
		try ( Connection conn = getConnection(m_conf) ) {
			// 주어진 경로명과 동일하거나, 앞은 동일하면서 더 긴 경로명의 데이터세트가 있는지
			// 조사한다.
			if ( findInGuard(conn, id) != null ) {
				if ( !force ) {
					throw new AnalysisExistsException(id);
				}
				
				removeInGuard(conn, Arrays.asList(id));
			}
			
			try ( PreparedStatement pstmt = conn.prepareStatement(SQL_INSERT_ANALYSIS) ) {
				pstmt.setString(1, id);
				pstmt.setString(2, analysis.getType().name());
				pstmt.setNull(3, Types.VARCHAR);
				pstmt.setNull(4, Types.VARCHAR);
				
				String argsExpr;
				switch ( analysis.getType() ) {
					case PLAN:
						Plan plan = ((PlanAnalysis)analysis).getPlan();
						ExecutePlanOptions opts = ((PlanAnalysis)analysis).getExecuteOptions();
						try {
							pstmt.setString(3, plan.toJson(true));
							pstmt.setString(4, PBUtils.toJson(opts.toProto(), true));
						}
						catch ( InvalidProtocolBufferException e ) {
							throw new AnalysisStoreException("invalid plan: " + plan);
						}
						break;
					case COMPOSITE:
						CompositeAnalysis mcompa = (CompositeAnalysis)analysis;
						for ( String subId: mcompa.getComponents() ) {
							if ( findInGuard(conn, subId) == null ) {
								throw new AnalysisNotFoundException(String.format("child analysis not found: composite=%s, child=%s", id, subId));
							}
						}
						String compStr = FStream.from(mcompa.getComponents()).join(";", ";", ";");
						pstmt.setString(3, compStr);
						break;
					case SYSTEM:
						SystemAnalysis sys = (SystemAnalysis)analysis;
						argsExpr = FStream.from(sys.getArguments()).join(";");
						pstmt.setString(3, sys.getFunctionId());
						pstmt.setString(4, argsExpr);
						break;
					case MODULE:
						ModuleAnalysis module = (ModuleAnalysis)analysis;
						argsExpr = KeyValueFStream.from(module.getArguments())
													.map(kv -> kv.toString())
													.join(";");
						pstmt.setString(3, module.getModuleId());
						pstmt.setString(4, argsExpr);
						break;
					case EXTERN:
						ExternAnalysis ext = (ExternAnalysis)analysis;
						argsExpr = FStream.from(ext.getArguments()).join(";", ";", ";");
						pstmt.setString(3, ext.getExecPath());
						pstmt.setString(4, argsExpr);
						break;
					default:
						throw new IllegalArgumentException("invalid MarmotAnalysis type: "
															+ analysis.getType());
				}
				
				if ( pstmt.executeUpdate() <= 0 ) {
					throw new AnalysisStoreException("fails to insert PlanAnalytics");
				}
			}
		}
		catch ( SQLException e ) {
			String state = e.getSQLState();
			if ( state.equals("23505") ) {
				throw new AnalysisExistsException(id);
			}
			
			throw new AnalysisStoreException(e);
		}
		catch ( IOException e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	public boolean remove(String id, boolean recursive) {
		try ( Connection conn = getConnection(m_conf) ) {
			CompositeAnalysis parent = findParentInGuard(conn, id);
			if ( parent != null ) {
				throw new AnalysisExistsException(parent.getId());
			}
			
			MarmotAnalysis anal = findInGuard(conn, id);
			if ( anal == null ) {
				return false;
			}
			
			List<MarmotAnalysis> descendants = null;
			if ( recursive && anal.getType() == Type.COMPOSITE ) {
				descendants = getSubAllInGuard(conn, id);
			}
			
			try ( PreparedStatement pstmt = conn.prepareStatement(SQL_DELETE_ANALYSIS); ) {
				pstmt.setString(1, id);
				int cnt = pstmt.executeUpdate();
				if ( cnt == 0 || descendants == null ) {
					return cnt > 0;
				}
			}
			removeInGuard(conn, FStream.from(descendants).map(MarmotAnalysis::getId).toList());
			
			return true;
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	public void removeAll() {
		try ( Connection conn = getConnection(m_conf) ) {
			try ( Statement stmt = conn.createStatement() ) {
				stmt.executeUpdate(SQL_DELETE_ALL);
			}
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	public CompositeAnalysis findParent(String id) {
		try ( Connection conn = getConnection(m_conf) ) {
			return findParentInGuard(conn, id);
		}
		catch ( Exception e ) {
			String msg = String.format("failed: MarmotAnalysisStore.findParent(%s), cause=%s", id, e);
			throw new AnalysisStoreException(msg);
		}
	}
	
	public List<CompositeAnalysis> getAncestorAll(String id) {
		List<CompositeAnalysis> ancestor = Lists.newArrayList();
		try ( Connection conn = getConnection(m_conf) ) {
			while ( true ) {
				CompositeAnalysis parent = findParentInGuard(conn, id);
				if ( parent == null ) {
					return ancestor;
				}
			
				ancestor.add(parent);
				id = parent.getId();
			}
		}
		catch ( Exception e ) {
			throw new AnalysisStoreException(e);
		}
	}
	
	public List<MarmotAnalysis> getDescendantAll(String id) {
		try ( Connection conn = getConnection(m_conf) ) {
			List<MarmotAnalysis> descendants = Lists.newArrayList();
			collectSubAllInGuard(conn, id, descendants);
			
			return descendants;
		}
		catch ( SQLException e ) {
			throw new JdbcException(e);
		}
	}

	private MarmotAnalysis findInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_GET_ANALYSIS); ) {
			pstmt.setString(1, id);
			
			return JdbcUtils.stream(pstmt.executeQuery(), s_toAnalysis)
							.findAny().orElse(null);
		}
	}
	
	private List<MarmotAnalysis> getAllOfTypeInGuard(Connection conn, Type type) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_LIST_ANALYSIS_OF_TYPE); ) {
			pstmt.setString(1, type.name());
			
			return JdbcUtils.stream(pstmt.executeQuery(), s_toAnalysis)
							.collect(Collectors.toList());
		}
	}
	
	private void removeInGuard(Connection conn, List<String> analysisList)
		throws SQLException {
		String deleteSql = FStream.from(analysisList)
									.map(id -> String.format("id='%s'", id))
									.join(" or ", SQL_DELETE_MANY, ";");
		
		try ( Statement stmt = conn.createStatement() ) {
			stmt.execute(deleteSql);
		}
	}
	
	private CompositeAnalysis findParentInGuard(Connection conn, String id) throws SQLException {
		try ( PreparedStatement pstmt = conn.prepareStatement(SQL_GET_DIRECT_SUPER) ) {
			pstmt.setString(1, String.format("%%;%s;%%", id));
			
			return FStream.from(JdbcUtils.stream(pstmt.executeQuery(), s_toAnalysis))
							.castSafely(CompositeAnalysis.class)
							.findFirst()
							.getOrNull();
		}
	}
	
	private void collectSubAllInGuard(Connection conn, String id, List<MarmotAnalysis> collecteds)
		throws SQLException {
		MarmotAnalysis analy = findInGuard(conn, id);
		if ( analy == null ) {
			throw new AnalysisNotFoundException(id);
		}
		if ( analy.getType() == Type.COMPOSITE ) {
			for ( String subId: ((CompositeAnalysis)analy).getComponents() ) {
				collectSubAllInGuard(conn, subId, collecteds);
			}
		}
		collecteds.add(analy);
	}
	
	private List<MarmotAnalysis> getSubAllInGuard(Connection conn, String id)
		throws SQLException {
		List<MarmotAnalysis> subs = Lists.newArrayList();
		Set<CompositeAnalysis> remains = Sets.newHashSet();
		
		MarmotAnalysis analy = findInGuard(conn, id);
		if ( analy == null ) {
			throw new AnalysisNotFoundException(id);
		}
		if ( analy.getType() != Type.COMPOSITE ) {
			return subs;
		}
		remains.add((CompositeAnalysis)analy);
		
		while ( !remains.isEmpty() ) {
			CompositeAnalysis composite = remains.iterator().next();
			remains.remove(composite);
			
			for ( String subId: composite.getComponents() ) {
				MarmotAnalysis sub = findInGuard(conn, subId);
				if ( sub != null ) {
					subs.add(sub);
					if ( sub.getType() == Type.COMPOSITE ) {
						remains.add((CompositeAnalysis)sub);
					}
				}
			}
		}
		
		return subs;
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
	
	private static final String SQL_CREATE_ANALYSIS
		= "create table analyses ("
		+ 	"id varchar not null,"
		+ 	"type varchar not null,"
		+ 	"func_id varchar,"
		+ 	"func_args varchar,"
		+ 	"primary key (id)"
		+ ")";

	private static final String SQL_GET_ANALYSIS
		= "select id, type, func_id, func_args "
		+ "from analyses where id=?";
	
	private static final String SQL_GET_ANALYSIS_ALL
		= "select id, type, func_id, func_args "
		+ "from analyses";
	
	private static final String SQL_LIST_ANALYSIS_OF_TYPE
		= "select id, type, func_id, func_args "
		+ "from analyses where type = ?";
	
	private static final String SQL_GET_DIRECT_SUPER
		= "select id, type, func_id, func_args "
		+ "from analyses where type = 'COMPOSITE' and func_id like ?";
	
	private static final String SQL_INSERT_ANALYSIS
		= "insert into analyses "
		+	"(id, type, func_id, func_args) "
		+	"values (?,?,?,?)";
	
	private static final String SQL_DELETE_ANALYSIS = "delete from analyses where id = ?";
	private static final String SQL_DELETE_MANY = "delete from analyses where ";
	private static final String SQL_DELETE_ALL = "delete from analyses";
	
	private static final CheckedFunctionX<ResultSet,MarmotAnalysis,SQLException> s_toAnalysis = rs -> {
		String planJson = null;
		try {
			String id = rs.getString(1);
			Type type = Type.valueOf(rs.getString(2));
			switch ( type ) {
				case PLAN:
					planJson = rs.getString(3);
					String optJson = rs.getString(4);
					return new PlanAnalysis(id, Plan.parseJson(planJson),
										ExecutePlanOptions.parseJson(optJson));
				case SYSTEM:
					String funcId = rs.getString(3);
					List<String> funcArgs = CSV.parseCsv(rs.getString(4), ';', '"')
												.toList(); 
					return new SystemAnalysis(id, funcId, funcArgs);
				case MODULE:
					String moduleId = rs.getString(3);
					String moduleArgs = rs.getString(4);
					Map<String,String> args = Utilities.parseKeyValueMap(moduleArgs, ';');
					return new ModuleAnalysis(id, moduleId, args);
				case COMPOSITE:
					String compCsv = rs.getString(3);
					List<String> compList = CSV.parseCsv(compCsv, ';')
												.drop(1).dropLast(1).toList();
					return new CompositeAnalysis(id, compList);
				case EXTERN:
					String path = rs.getString(3);
					List<String> cmdArgs = CSV.parseCsv(rs.getString(4), ';', '"')
												.drop(1).dropLast(1)
												.toList(); 
					return new ExternAnalysis(id, path, cmdArgs);
				default:
					throw new AssertionError();
			}
		}
		catch ( IOException e ) {
			throw new AnalysisStoreException("invalid plan: " + planJson);
		}
		catch ( SQLException e ) {
			throw new AnalysisStoreException(e);
		}
	};
}
