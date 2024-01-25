package marmot.command;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.MarmotServer;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import utils.StopWatch;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="upload_files",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="upload local files into HDFS")
public class HadoopUploadFilesMain extends UploadFilesCommand {
	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		HadoopUploadFilesMain cmd = new HadoopUploadFilesMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}
			else {
				StopWatch watch = StopWatch.start();

				MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
				MarmotServer server = new MarmotServer(marmot);
				try {
					cmd.accept(server);
					System.out.printf("uploaded: elapsed=%s%n", watch.getElapsedMillisString());
				}
				finally {
					server.shutdown();
				}
			}
		}
		catch ( Exception e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
