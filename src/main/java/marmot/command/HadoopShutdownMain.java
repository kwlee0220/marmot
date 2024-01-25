package marmot.command;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import utils.UsageHelp;
import utils.func.FOption;
import utils.func.UncheckedConsumer;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="shutdown_marmot",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="marmot-server shutdown")
public class HadoopShutdownMain implements Runnable {
	private final static Logger s_logger = LoggerFactory.getLogger(HadoopShutdownMain.class);
	
	@Mixin private UsageHelp m_help;
	
	@Option(names={"-home"}, paramLabel="path", description={"MarmotServer home directory"})
	private String m_homeDir;
	
	@Option(names={"-lock"}, paramLabel="path", description={"MarmotServer termination-lock file"})
	private String m_lock = null;

	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		HadoopShutdownMain cmd = new HadoopShutdownMain();
		CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF);
	}

	@Override
	public void run() {
		FOption.ofNullable(m_lock)
				.map(File::new)
				.orElse(() -> getHomeDir().map(dir -> new File(dir, ".lock")))
				.ifPresent(UncheckedConsumer.sneakyThrow(this::touch));
	}
	
	private void touch(File lock) throws IOException {
		s_logger.info("try to terminate the MarmotServer: lock={}", lock.getAbsolutePath());
		Files.touch(lock);
	}
	
	private FOption<File> getHomeDir() {
		return FOption.ofNullable(m_homeDir)
						.orElse(() -> FOption.ofNullable(System.getenv("MARMOT_SERVER_HOME")))
						.map(File::new);
	}
}
