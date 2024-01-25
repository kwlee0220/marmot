package marmot.optor.geo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.MarmotServer;
import marmot.RecordSet;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.grid.Parameters;
import marmot.mapreduce.input.grid.SquareGridFileInputFormat;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.rset.GridRecordSet;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.proto.optor.LoadSquareGridFileProto;
import marmot.support.PBSerializable;
import utils.Size2d;
import utils.Size2i;
import utils.Size2l;
import utils.Utilities;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadSquareGridFile extends AbstractRecordSetLoader
								implements MapReduceableRecordSetLoader,
											PBSerializable<LoadSquareGridFileProto> {
	private static final Logger s_logger = LoggerFactory.getLogger(LoadSquareGridFile.class);

	private final SquareGrid m_grid;
	private Envelope m_bounds;
	private FOption<Integer> m_nworkers = FOption.empty();	// 적재작업을 수행할 파티션(mapper)의 수
	
	public LoadSquareGridFile(SquareGrid grid) {
		Utilities.checkNotNullArgument(grid, "SquareGrid should not be null");

		m_grid = grid;
		setLogger(s_logger);
	}
	
	public LoadSquareGridFile(String dataset, Size2d cellSize) {
		Utilities.checkNotNullArgument(dataset, "dataset id should not be null");
		Utilities.checkNotNullArgument(cellSize, "Grid cell size should not be null");

		m_grid = new SquareGrid(dataset, cellSize);
		setLogger(s_logger);
	}
	
	public LoadSquareGridFile(Envelope bounds, Size2d cellSize) {
		Utilities.checkNotNullArgument(bounds, "Universe Envelope should not be null");
		Utilities.checkNotNullArgument(cellSize, "Grid cell size should not be null");

		m_grid = new SquareGrid(bounds, cellSize);
		setLogger(s_logger);
	}
	
	public LoadSquareGridFile setWorkerCount(int cnt) {
		Preconditions.checkArgument(cnt > 0, "worker count should be larger than zero");
		
		m_nworkers = FOption.of(cnt);
		return this;
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		if ( m_bounds == null ) {
			MarmotServer server = new MarmotServer(marmot);
			m_bounds = m_grid.getGridBounds(server);
		}
		
		setInitialized(marmot, GridRecordSet.SCHEMA);
	}

	@Override
	public String getInputString() {
		return String.format("%s (nparts=%s)", toCellSizeString(), m_nworkers);
	}

	@Override
	public void configure(Job job) {
		Configuration conf = job.getConfiguration();
		
		// 입력 파일 포맷 설정
		job.setInputFormatClass(SquareGridFileInputFormat.class);
		SquareGridFileInputFormat.setParameters(conf, getParameters());
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		checkInitialized();
		
		int length = m_nworkers.getOrElse(() -> getParameters().getSplitCount());
		if ( length >= 3 ) {
			return PlanMRExecutionMode.NON_LOCALLY_EXECUTABLE;
		}
		if ( length == 1 ) {
			return PlanMRExecutionMode.LOCALLY_EXECUTABLE;
		}
		else {
			return PlanMRExecutionMode.CONDITIONALLY_EXECUTABLE;
		}
	}
	
	@Override
	public RecordSet load() {
		return new GridRecordSet(m_bounds, 0, new Size2i(1, 1), m_grid.getCellSize());
	}
	
	@Override
	public String toString() {
		if ( m_grid.getGridBounds().isRight() ) {
			Envelope bounds = m_grid.getGridBounds().right().get();
			
			long width = (long)Math.ceil(bounds.getWidth() / m_grid.getCellSize().getWidth());
			long height = (long)Math.ceil(bounds.getHeight() / m_grid.getCellSize().getHeight());
			Size2l dim = new Size2l(width, height);
			
			return String.format("load_grid[dim=%s, cell=%s, npart=%s]",
									dim, toCellSizeString(), m_nworkers);
		}
		else {
			return String.format("load_grid[dataset=%s, cell=%s, npart=%s]",
									m_grid.getGridBounds().getLeft(),
									toCellSizeString(), m_nworkers);
		}
	}

	public static LoadSquareGridFile fromProto(LoadSquareGridFileProto proto) {
		SquareGrid grid = SquareGrid.fromProto(proto.getGrid());
		LoadSquareGridFile op = new LoadSquareGridFile(grid);
		
		switch ( proto.getOptionalSplitCountCase() ) {
			case SPLIT_COUNT:
				op.setWorkerCount(proto.getSplitCount());
				break;
			default:
		}
		
		return op;
	}
	
	public LoadSquareGridFileProto toProto() {
		LoadSquareGridFileProto.Builder builder = LoadSquareGridFileProto.newBuilder()
															.setGrid(m_grid.toProto());
		m_nworkers.ifPresent(builder::setSplitCount);
		
		return builder.build();
	}
	
	private Parameters getParameters() {
		Parameters params = new Parameters(m_bounds, m_grid.getCellSize());
		m_nworkers.ifPresent(params::setSplitCount);
		
		return params;
	}
	
	private String toCellSizeString() {
		Size2d cellSize = m_grid.getCellSize();
		return String.format("%.0fmX%.0fm", cellSize.getWidth(), cellSize.getHeight());
	}
}
