package marmot.module;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;

import marmot.MarmotInternalException;
import marmot.module.geo.ClusterWithKMeans;
import marmot.module.geo.DBSCAN;
import marmot.module.geo.E2SFCA;
import marmot.module.geo.arc.ArcBufferProcess;
import marmot.module.geo.arc.ArcMergeProcess;
import marmot.module.geo.arc.ArcSplitProcess;
import marmot.module.geo.arc.ArcUnionProcess;
import utils.Throwables;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotModuleRegistry {
	private static final MarmotModuleRegistry SINGLETON = new MarmotModuleRegistry();
	
	private final Map<String,Class<? extends MarmotModule>> m_registry = Maps.newHashMap();
	
	public static final MarmotModuleRegistry get() {
		return SINGLETON;
	}
	
	public MarmotModuleRegistry() {
		m_registry.put("kmeans", ClusterWithKMeans.class);
		m_registry.put("e2sfca", E2SFCA.class);
		m_registry.put("normalize", Normalize.class);
		m_registry.put("percentile_rank", PercentileRank.class);
		m_registry.put("dbscan", DBSCAN.class);
		m_registry.put("arc_split", ArcSplitProcess.class);
		m_registry.put("arc_merge", ArcMergeProcess.class);
		m_registry.put("arc_buffer", ArcBufferProcess.class);
		m_registry.put("arc_union", ArcUnionProcess.class);
	}
	
	public Set<String> getModuleAnalysisClassAll() {
		return m_registry.keySet();
	}
	
	@SuppressWarnings("unchecked")
	public List<String> getModuleParameterNameAll(String id) {
		Class<? extends MarmotModule> moduleCls = m_registry.get(id);
		if ( moduleCls != null ) {
			try {
				MarmotModule analy = (MarmotModule)moduleCls.newInstance();
				
				Method method = moduleCls.getMethod("getParameterNameAll");
				return (List<String>)method.invoke(analy);
			}
			catch ( Exception e ) {
				String msg = String.format("fails to call 'getParameterNameAll': id=%s, class=%s, cause=%s",
											id, moduleCls, e);
				throw new MarmotInternalException(msg);
			}
		}
		else {
			throw new IllegalArgumentException("unregistered module: id=" + id);
		}
	}
	
	public MarmotModule createModule(String id) {
		Utilities.checkNotNullArgument(id, "module id is null");
		
		Class<? extends MarmotModule> procCls = m_registry.get(id);
		if ( procCls == null ) {
			throw new MarmotModuleException("unregistered MarmotModule: id=" + id);
		}
		
		try {
			return procCls.newInstance();
		}
		catch ( Throwable e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			throw new MarmotModuleException("fails to create MarmotModule: id=" + id
											+ ", cause=" + cause);
		}
	}
}
