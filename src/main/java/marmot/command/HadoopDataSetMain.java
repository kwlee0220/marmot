package marmot.command;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;


/**
 * </ol>
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="dataset-related commands",
		subcommands = {
			DatasetCommands.ListDataSet.class,
			DatasetCommands.Show.class,
			DatasetCommands.Schema.class,
			DatasetCommands.Move.class,
			DatasetCommands.SetGcInfo.class,
			DatasetCommands.Count.class,
			DatasetCommands.Bind.class,
			DatasetCommands.Delete.class,
			DatasetCommands.Import.class,
			DatasetCommands.Export.class,
			DatasetCommands.Thumbnail.class,
		})
public class HadoopDataSetMain extends MarmotServerCommand {
	protected HadoopDataSetMain(String[] args) throws Exception {
		super(args);
	}

	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		HadoopDataSetMain cmd = new HadoopDataSetMain(args);
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, cmd.getApplicationArguments());
		}
		finally {
			cmd.getInitialContext().shutdown();
		}
	}
}
