package marmot.remote.protobuf;

import static java.util.concurrent.CompletableFuture.runAsync;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.logging.LogManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;

import utils.NetUtils;
import utils.func.UncheckedRunnable;

import marmot.MarmotCore;
import marmot.MarmotServer;
import marmot.command.MarmotServerCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help;
import picocli.CommandLine.Option;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
@Command(name="marmot_server",
		parameterListHeading = "Parameters:%n",
		optionListHeading = "Options:%n",
		description="start MarmotServer")
public class PBMarmotServerMain extends MarmotServerCommand {
	private final static Logger s_logger = LoggerFactory.getLogger(PBMarmotServerMain.class);
	
	private static final int DEFAULT_MARMOT_PORT = 12985;
	
	@Option(names={"-port"}, paramLabel="number", required=false,
			description={"marmot server port number"})
	private int m_port = -1;
	
	@Option(names={"-compress"}, description="compress intermediate data")
	private boolean m_compress;
	
	protected PBMarmotServerMain(String[] args) throws Exception {
		super(args);
	}
	
	public static final void main(String... args) throws Exception {
		File propsFile = MarmotServerCommand.configureStaticLog4j();
		System.out.printf("loading marmot log4j.properties: %s%n", propsFile.getAbsolutePath());

		PBMarmotServerMain cmd = new PBMarmotServerMain(args);
		try {
			CommandLine.run(cmd, System.out, System.err, Help.Ansi.OFF, cmd.getApplicationArguments());
		}
		finally {
			cmd.getInitialContext().shutdown();
		}
	}
	
	@Override
	protected void run(MarmotServer marmot) throws IOException, InterruptedException {
		LogManager.getLogManager().reset();
		
		int port = m_port;
		if ( port < 0 ) {
			String portStr = System.getenv("MARMOT_PORT");
			port = (portStr != null) ? Integer.parseInt(portStr) : DEFAULT_MARMOT_PORT;
		}
		
		Server server = createServer(marmot, port);
		server.start();

		String host = NetUtils.getLocalHostAddress();
		System.out.printf("started: MarmotServer[host=%s, port=%d]%n", host, port);

		getTerminationLockFile()
			.orElse(() -> getHomeDir().map(dir -> new File(dir, ".lock")))
			.ifPresent(lock -> {
				s_logger.info("monitor the termination_lock: {}", lock.getAbsolutePath());
				runAsync(UncheckedRunnable.sneakyThrow(() -> awaitTermination(server, lock)));
			});
		server.awaitTermination();
	}
	
	private void awaitTermination(Server server, File lockFile) throws IOException, InterruptedException {
		String lockPathStr = lockFile.getAbsolutePath();
		
		WatchService watch = FileSystems.getDefault().newWatchService();
		File parent = lockFile.getParentFile();
		parent.toPath().register(watch, StandardWatchEventKinds.ENTRY_MODIFY);
		
        WatchKey key;
		while ( (key = watch.take()) != null ) {
			for ( WatchEvent<?> ev : key.pollEvents() ) {
				if ( ev.kind() == StandardWatchEventKinds.ENTRY_MODIFY ) {
					Path target = Paths.get(parent.getAbsolutePath(), ev.context().toString());
					if ( lockPathStr.equals(target.toString())) {
						server.shutdown();
						
						s_logger.info("terminating the MarmotServer: lock={}",
										lockFile.getAbsolutePath());
						
						return;
					}
				}
			}
			key.reset();
		}
	}
	
	private Server createServer(MarmotServer server, int port) {
		MarmotCore marmot = server.getMarmotCore();
		
		PBFileServiceServant fileServant = new PBFileServiceServant(marmot);
		PBDataSetServiceServant dsServant = new PBDataSetServiceServant(marmot);
		PBPlanExecutionServiceServant pexecServant = new PBPlanExecutionServiceServant(server);
		
		Server nettyServer = NettyServerBuilder.forPort(port)
					//							.addService(rsetServant)
												.addService(fileServant)
												.addService(dsServant)
												.addService(pexecServant)
												.build();
		pexecServant.setGrpcServer(nettyServer);
		return nettyServer;
	}
}
