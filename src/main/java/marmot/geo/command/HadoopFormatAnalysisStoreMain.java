package marmot.geo.command;

import org.apache.hadoop.conf.Configuration;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.command.MarmotServerCommand;
import marmot.exec.MarmotAnalysisStore;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="bind_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="format MarmotAnalyticsStore")
public class HadoopFormatAnalysisStoreMain {
	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		HadoopFormatAnalysisStoreMain cmd = new HadoopFormatAnalysisStoreMain();
		CommandLine commandLine = new CommandLine(cmd).setUsageHelpWidth(100);
		try {
			commandLine.parse(args);
			
			if ( commandLine.isUsageHelpRequested() ) {
				commandLine.usage(System.out, Ansi.OFF);
			}

			MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
			Configuration conf = marmot.getHadoopConfiguration();
			
			MarmotAnalysisStore.dropStore(conf);
			MarmotAnalysisStore.createStore(conf);
		}
		catch ( Throwable e ) {
			System.err.println(e);
			commandLine.usage(System.out, Ansi.OFF);
		}
	}
}
