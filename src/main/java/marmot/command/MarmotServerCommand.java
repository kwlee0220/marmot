package marmot.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.MarmotServer;
import picocli.CommandLine;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import utils.PicocliCommand;
import utils.UsageHelp;
import utils.func.FOption;
import utils.func.Tuple;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotServerCommand implements PicocliCommand {
	private static final Logger s_logger = LoggerFactory.getLogger(MarmotServerCommand.class);
	
	@Spec private CommandSpec m_spec;
	@Mixin private UsageHelp m_help;
	
	@Option(names={"-home"}, paramLabel="path", description={"MarmotServer home directory"})
	private String m_homeDir;
	
	@Option(names={"-config"}, paramLabel="path", description={"MarmotServer config directory"})
	private String m_configDir;
	
	@Option(names={"-lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	private String m_lock = null;
	
	@Option(names={"-runner"}, paramLabel="runner mode", description={"local|local-mr|mr"})
	private String m_runnerMode = "mr";
	
	@Option(names={"-run_at_cluster"}, paramLabel="boolean",
			description={"run this command at cluster"})
	private boolean m_runAtCluster = false;
	
	private MarmotServer m_marmot;
	private String[] m_applArgs;
	
	protected void run(MarmotServer marmot) throws Exception { }
	
	private MarmotServerCommand() { }
	
	protected MarmotServerCommand(String... args) throws Exception {
		MarmotServerCommand cmd = new MarmotServerCommand();
		CommandLine cmdLine = new CommandLine(cmd).setUnmatchedArgumentsAllowed(true);
		cmdLine.parse(args);
		
		MarmotCoreBuilder builder = new MarmotCoreBuilder().setRunnerMode(cmd.m_runnerMode);
		if ( cmd.m_configDir != null ) {
			builder = builder.setConfigDir(new File(cmd.m_configDir));
		}
		else if ( cmd.m_homeDir != null ) {
			builder = builder.setHomeDir(new File(cmd.m_homeDir));
		}
		builder.runAtCluster(cmd.m_runAtCluster);
		
		Tuple<MarmotCore,String[]> ctx = builder.build(args);
		m_marmot = new MarmotServer(ctx._1);
		m_applArgs = ctx._2;
	}
	
	public FOption<File> getHomeDir() {
		return FOption.ofNullable(m_homeDir)
						.orElse(() -> FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME")))
						.map(File::new);
	}
	
	public FOption<File> getConfigDir() {
		return FOption.ofNullable(m_configDir).map(File::new);
	}
	
	public FOption<File> getTerminationLockFile() {
		return FOption.ofNullable(m_lock).map(File::new);
	}
	
	@Override
	public MarmotServer getInitialContext() {
		return m_marmot;
	}
	
	public String[] getApplicationArguments() {
		return m_applArgs;
	}
	
	@Override
	public void run() {
		try {
			run(getInitialContext());
		}
		catch ( Exception e ) {
			System.err.printf("failed: %s%n%n", e);
			
			m_spec.commandLine().usage(System.out, Ansi.OFF);
		}
	}

	@Override
	public void configureLog4j() throws IOException {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		File propsFile = new File(homeDir, "log4j.properties");
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		if ( s_logger.isDebugEnabled() ) {
			s_logger.debug("use log4j.properties from {}", propsFile);
		}
	}
	
	public static File getLog4jPropertiesFile() {
		String homeDir = FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME"))
								.getOrElse(() -> System.getProperty("user.dir"));
		return new File(homeDir, "log4j.properties");
	}
	
	public static File configureStaticLog4j() throws IOException {
		File propsFile = getLog4jPropertiesFile();
		
		Properties props = new Properties();
		try ( InputStream is = new FileInputStream(propsFile) ) {
			props.load(is);
		}
		
		Map<String,String> bindings = Maps.newHashMap();
		bindings.put("marmot.home", propsFile.getParentFile().toString());

		String rfFile = props.getProperty("log4j.appender.rfout.File");
		rfFile = StringSubstitutor.replace(rfFile, bindings);
		props.setProperty("log4j.appender.rfout.File", rfFile);
		
		return propsFile;
	}
}