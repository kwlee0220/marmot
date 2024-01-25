package marmot.mapreduce;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang.text.StrSubstitutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.exec.PlanExecution;
import marmot.exec.PlanExecutionFactory;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ProcessPlanExecutionFactory implements PlanExecutionFactory {
	private static final String YARN_PATH = "/usr/bin/yarn";
	private static final String MARMOT_JAR_PATH = "${marmot.home.dir}/bin/marmot.jar";
	private static final String EXEC_PROGRAM = "marmot.command.PlanExecutorMain";

	private final MarmotCore m_marmot;
	private final File m_homeDir;
	private final List<File> m_libJarFiles = Lists.newArrayList();
	private final String m_yarnScriptPath;
	private final String m_jarPath;
	
	public static ProcessPlanExecutionFactory of(MarmotCore marmot) {
		File homeDir = new File(System.getenv("MARMOT_SERVER_HOME"));
		return new ProcessPlanExecutionFactory(marmot, homeDir);
	}
	
	public static ProcessPlanExecutionFactory of(MarmotCore marmot, File homeDir) {
		return new ProcessPlanExecutionFactory(marmot, homeDir);
	}
	
	public ProcessPlanExecutionFactory(MarmotCore marmot, File homeDir) {
		Utilities.checkNotNullArgument(marmot, "MarmotRuntime");
		Utilities.checkNotNullArgument(homeDir, "Marmot home directory");
		
		m_marmot = marmot;
		m_homeDir = homeDir;
		
		Map<String,String> props = Maps.newHashMap();
		props.put("marmot.home.dir", homeDir.getAbsolutePath());
		
		m_yarnScriptPath = StrSubstitutor.replace(YARN_PATH, props);
		m_jarPath = StrSubstitutor.replace(MARMOT_JAR_PATH, props);
	}

	@Override
	public PlanExecution create(Plan plan) {
		List<String> command = Lists.newArrayList();
		
		command.add(m_yarnScriptPath);	// yarn
		command.addAll(Arrays.asList("jar", m_jarPath, EXEC_PROGRAM));
		
		if ( m_libJarFiles.size() > 0 ) {
			String libJars = m_libJarFiles.stream()
											.map(File::getAbsolutePath)
											.collect(Collectors.joining(","));
			command.addAll(Arrays.asList("-libjars", libJars));
		}
		command.addAll(Arrays.asList("-plan_runner", "mr"));
		command.addAll(Arrays.asList("-marmotHomeDir", m_homeDir.getAbsolutePath()));
		
		RecordSchema outSchema = m_marmot.getOutputRecordSchema(plan);
		return new ProcessPlanExecution(command, plan, outSchema);
	}
	
	public void addLibJarFile(File jarFile) {
		m_libJarFiles.add(jarFile);
	}
}
