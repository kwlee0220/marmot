package marmot.geo.command;

import marmot.MarmotServer;
import marmot.command.MarmotServerCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.StopWatch;
import utils.UnitUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="cluster_dataset",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="cluster a dataset")
public class HadoopClusterDataSetMain extends MarmotServerCommand {
	@Parameters(paramLabel="dataset", index="0", arity="1..1",
				description={"dataset id to cluster"})
	private String m_dsId;

	@Option(names= {"-sample_size"}, paramLabel="nbytes", description="sample size (eg: '64mb')")
	private void setSampleSize(String sizeStr) {
		m_sampleSize = UnitUtils.parseByteSize(sizeStr);
	}
	private long m_sampleSize = -1;

	@Option(names= {"-b", "-block_size"}, paramLabel="nbytes", description="block size (eg: '64mb')")
	private void setBlockSize(String sizeStr) {
		m_blockSize = UnitUtils.parseByteSize(sizeStr);
	}
	private long m_blockSize = -1;
	
	@Option(names="-workers", paramLabel="count", description="reduce task count")
	private int m_nworkers = -1;

	@Option(names="-plan_runner", paramLabel="local|local-mr|mr|tez", required=true,
			description="plan execution mode")
	private String m_planRunner = "mr";
	
	protected HadoopClusterDataSetMain(String[] args) throws Exception {
		super(args);
	}

	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		HadoopClusterDataSetMain cmd = new HadoopClusterDataSetMain(args);
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, cmd.getApplicationArguments());
		}
		finally {
			cmd.getInitialContext().shutdown();
		}
	}
	
	@Override
	public void run(MarmotServer marmot) {
		StopWatch watch = StopWatch.start();
		
		CreateSpatialIndexOptions options = CreateSpatialIndexOptions.DEFAULT();
		if ( m_sampleSize > 0 ) {
			options = options.sampleSize(m_sampleSize);
		}
		if ( m_blockSize > 0 ) {
			options = options.blockSize(m_blockSize);
		}
		if ( m_nworkers > 0 ) {
			options = options.workerCount(m_nworkers);
		}
		
		marmot.getDataSet(m_dsId).createSpatialIndex(options);
		
		System.out.println("elapsed time: " + watch.stopAndGetElpasedTimeString());
	}
}
