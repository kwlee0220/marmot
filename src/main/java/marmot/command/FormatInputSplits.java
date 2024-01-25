package marmot.command;

import org.apache.hadoop.conf.Configuration;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.mapreduce.input.fixed.InputSplitStore;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FormatInputSplits {
	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
		Configuration conf = marmot.getHadoopConfiguration();
		
		InputSplitStore.dropStore(conf);
		InputSplitStore.createStore(conf);
	}
}
