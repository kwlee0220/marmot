package marmot.geo.command;

import org.apache.hadoop.conf.Configuration;

import marmot.MarmotCore;
import marmot.MarmotCoreBuilder;
import marmot.command.MarmotServerCommand;
import marmot.dataset.Catalog;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class FormatCatalog {
	public static final void main(String... args) throws Exception {
		MarmotServerCommand.configureStaticLog4j();

		MarmotCore marmot = new MarmotCoreBuilder().forLocalMR().build();
		Configuration conf = marmot.getHadoopConfiguration();

		Catalog.dropCatalog(conf);
		Catalog.createCatalog(conf);
	}
}
