package marmot;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Tuple;
import utils.Utilities;
import utils.func.FOption;

/**
 * MarmotServer builder 클래스.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotCoreBuilder {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotCoreBuilder.class);
	private static final String[] EMPTY_ARGS = new String[0];
	private static final String HADOOP_CONFIG = "hadoop-conf";
	
	private FOption<File> m_configDir = FOption.empty();
	private String m_runnerMode;
	private boolean m_runAtCluster = false;
	
	public MarmotCoreBuilder setHomeDir(File dir) {
		Utilities.checkArgument(dir.isDirectory(), "HomeDir is not a directory: " + dir);
		
		return setConfigDir(new File(dir, HADOOP_CONFIG));
	}
	
	public MarmotCoreBuilder setConfigDir(File dir) {
		Utilities.checkArgument(dir.isDirectory(), "ConfigDir is not a directory: " + dir);
		
		m_configDir = FOption.of(dir);
		return this;
	}
	
	public MarmotCoreBuilder setRunnerMode(String mode) {
		m_runnerMode = mode;
		return this;
	}
	
	public MarmotCoreBuilder runAtCluster(boolean flag) {
		m_runAtCluster = flag;
		return this;
	}
	
	public MarmotCore build() {
		try {
			Driver driver = new Driver();
			ToolRunner.run(driver, EMPTY_ARGS);
			
			return build(driver.getConf(), driver.m_args)._1;
		}
		catch ( Exception e ) {
			throw new RuntimeException(e);
		}
	}
	
	public Tuple<MarmotCore,String[]> build(String... args) throws Exception {
		Utilities.checkNotNullArgument(args != null, "empty arguments");
		
		Driver driver = new Driver();
		ToolRunner.run(driver, args);
		
		return build(driver.getConf(), driver.m_args);
	}
	
	public MarmotCoreBuilder forMR() {
		return setRunnerMode("mr");
	}
	
	public MarmotCoreBuilder forLocalMR() {
		return setRunnerMode("local-mr");
	}
	
	public MarmotCoreBuilder forLocal() {
		return setRunnerMode("local");
	}
	
	public static String[] toApplicationArguments(String... args) throws Exception {
		Driver driver = new Driver();
		ToolRunner.run(driver, EMPTY_ARGS);
		
		return driver.m_args;
	}
	
	private InputStream readMarmotResource(String name) throws FileNotFoundException {
		if ( m_configDir.isPresent() ) {
			return new FileInputStream(new File(m_configDir.getUnchecked(), name));
		}
		else {
			name = String.format("%s/%s", HADOOP_CONFIG, name);
			return Thread.currentThread().getContextClassLoader()
										.getResourceAsStream(name);
		}
	}
	
	private Tuple<MarmotCore,String[]> build(Configuration conf, String[] applArgs)
		throws Exception {
		Utilities.checkNotNullArgument(m_runnerMode != null, "runner mode is not specified");
		
		if ( !m_runAtCluster && m_configDir.isAbsent() ) {
			setHomeDir(getHomeDir().getOrThrow(() -> new IllegalStateException("Marmot config directory is not specified")));
		}
		
		// Marmot 설정 정보 추가
		conf.addResource(readMarmotResource("marmot.xml"));
		
		// Marmot runner 설정 추가
		switch ( m_runnerMode ) {
			case "local":
			case "local-mr":
			case "mr":
				conf.addResource(readMarmotResource(String.format("marmot-%s.xml", m_runnerMode)));
				break;
			default:
				throw new IllegalArgumentException("invalid plan runner: " + m_runnerMode);
		}
		
		MarmotCore marmot = new MarmotCore(conf);
		return Tuple.of(marmot, applArgs);
	}
	
	private FOption<File> getHomeDir() {
		return FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
						.map(File::new);
	}

	private static class Driver extends Configured implements Tool {
		private String[] m_args;
		
		@Override
		public int run(String[] args) throws Exception {
			m_args = args;
			return 0;
		}
	}
}
