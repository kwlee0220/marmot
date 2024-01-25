package marmot.optor.geo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.mapreduce.PlanMRExecutionMode;
import marmot.mapreduce.input.hexagrid.HexaGridSplit;
import marmot.mapreduce.input.hexagrid.HexagonGridFileInputFormat;
import marmot.optor.MapReduceableRecordSetLoader;
import marmot.optor.RecordSetOperatorException;
import marmot.optor.rset.GridRecordSet;
import marmot.optor.support.AbstractRecordSetLoader;
import marmot.proto.optor.LoadHexagonGridFileProto;
import marmot.proto.optor.LoadHexagonGridFileProto.GridBoundsProto;
import marmot.protobuf.PBUtils;
import marmot.support.PBSerializable;
import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class LoadHexagonGridFile extends AbstractRecordSetLoader
								implements MapReduceableRecordSetLoader,
											PBSerializable<LoadHexagonGridFileProto> {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(LoadHexagonGridFile.class);

	private String m_dataset;	// m_dataset != null || (m_srid != null && m_bounds != null)
	private String m_srid;
	private Envelope m_bounds;
	private final Size2i m_partDim;
	private final double m_sideLength;
	
	private LoadHexagonGridFile(String srid, Envelope bounds, Size2i partDim,
								double sideLen) {
		Preconditions.checkArgument(srid != null, "SRID should not be null");
		Preconditions.checkArgument(bounds != null, "Bounds should not be null");
		Preconditions.checkArgument(partDim != null, "Grid partition dimension should not be null");
		Preconditions.checkArgument(sideLen > 0, "sideLen > 0");

		m_srid = srid;
		m_bounds = bounds;
		m_partDim = partDim;
		m_sideLength = sideLen;
	}
	
	private LoadHexagonGridFile(String dataset, Size2i partDim, double sideLen) {
		Preconditions.checkArgument(dataset != null, "dataset id should not be null");
		Preconditions.checkArgument(partDim != null, "Grid partition dimension should not be null");
		Preconditions.checkArgument(sideLen > 0, "sideLen > 0");

		m_dataset = dataset;
		m_partDim = partDim;
		m_sideLength = sideLen;
	}
	
	@Override
	public void initialize(MarmotCore marmot) {
		setInitialized(marmot, GridRecordSet.SCHEMA);
	}

	@Override
	public String getInputString() {
		return String.format("%s (%s)", m_sideLength, m_partDim);
	}

	@Override
	public void configure(Job job) {
		// 입력 파일 포맷 설정
		job.setInputFormatClass(HexagonGridFileInputFormat.class);

		// 기타 설정
		Configuration conf = job.getConfiguration();
		if ( m_dataset != null ) {
			HexagonGridFileInputFormat.setDataSetId(conf, m_dataset);
		}
		if ( m_srid != null ) {
			HexagonGridFileInputFormat.setSRID(conf, m_srid);
			HexagonGridFileInputFormat.setGridBounds(conf, m_bounds);
		}
		HexagonGridFileInputFormat.setPartionDimension(conf, m_partDim);
		HexagonGridFileInputFormat.setSideLength(conf, m_sideLength);
	}

	@Override
	public PlanMRExecutionMode getExecutionMode(MarmotCore marmot) throws IOException {
		int length = m_partDim.getArea();
		
		if ( length > 4 ) {
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
		checkInitialized();
		
		String srid = m_srid;
		Envelope bounds = m_bounds;
		if ( m_dataset != null ) {
			DataSet ds = m_marmot.getDataSet(m_dataset);
			if ( ds.hasGeometryColumn() ) {
				srid = ds.getGeometryColumnInfo().srid();
			}
			bounds = ds.getBounds();
		}
		
		return new HexaGridSplit(0, m_partDim.getWidth(), srid, bounds, bounds,
								m_sideLength).getRecordSet();
	}
	
	@Override
	public String toString() {
		return String.format("load_grid[part=%s, side_len=%f]", m_partDim, m_sideLength);
	}

	public static LoadHexagonGridFile fromProto(LoadHexagonGridFileProto proto) {
		LoadHexagonGridFile.Builder builder = LoadHexagonGridFile.builder()
														.sideLength(proto.getSideLength());
		switch ( proto.getGridBoundsCase() ) {
			case DATASET:
				builder.dataset(proto.getDataset());
				break;
			case BOUNDS:
				GridBoundsProto boundsProto = proto.getBounds();
				builder.gridBounds(PBUtils.fromProto(boundsProto.getBounds()));
				builder.srid(boundsProto.getSrid());
				break;
			default:
	 			throw new RecordSetOperatorException(String.format("op=%s, cause=%s",
				 										LoadHexagonGridFile.class,
				 										"grid bounds does not specified for "));
		}
		
		switch ( proto.getPartSizeCase() ) {
			case PART_DIM:
				Size2i partDim = PBUtils.fromProto(proto.getPartDim());
				builder.partitionDimension(partDim);
				break;
			case PART_COUNT:
				builder.partitionCount(proto.getPartCount());
				break;
			default:
				throw new RecordSetOperatorException("partition size does not specified for "
													+ LoadHexagonGridFile.class.getSimpleName());
		}
		
		return builder.build();
	}
	
	public LoadHexagonGridFileProto toProto() {
		LoadHexagonGridFileProto.Builder builder = LoadHexagonGridFileProto.newBuilder()
													.setPartDim(PBUtils.toProto(m_partDim))
													.setSideLength(m_sideLength);
		
		if ( m_dataset != null ) {
			builder.setDataset(m_dataset);
		}
		else {
			builder.setBounds(GridBoundsProto.newBuilder()
											.setBounds(PBUtils.toProto(m_bounds))
											.setSrid(m_srid)
											.build());
		}
		
		return builder.build();
	}
	
	public static Builder builder() {
		return new Builder();
	}
	public static class Builder {
		private String m_dataset;
		private String m_srid;
		private Envelope m_bounds;
		private Size2i m_partDim;
		private double m_sideLength;
		
		public LoadHexagonGridFile build() {
			if ( m_dataset != null ) {
				return new LoadHexagonGridFile(m_dataset, m_partDim, m_sideLength);
			}
			else {
				return new LoadHexagonGridFile(m_srid, m_bounds, m_partDim, m_sideLength);
			}
		}

		public Builder dataset(String dataset) {
			m_dataset = dataset;
			return this;
		}

		public Builder srid(String srid) {
			m_srid = srid;
			return this;
		}
		
		/**
		 * 사각 grid를 생성할 전체 {@link Envelope}를 지정한다.
		 * 
		 * @param envelope	전체 영역
		 * @return	자신 빌더 객체.
		 */
		public Builder gridBounds(Envelope envelope) {
			m_bounds = envelope;
			return this;
		}
		
		/**
		 * 생성할 각 grid 셀의 한 면의 길이를 설정한다.
		 * 
		 * @param length	면 길이 (단위: 미터)
		 * @return	자신 빌더 객체.
		 */
		public Builder sideLength(double length) {
			m_sideLength = length;
			return this;
		}
		
		/**
		 * 생설될 전체 grid 셀의 갯수를 설정한다.
		 * 
		 * @param dim	셀의 갯수 (가로 x 세로)
		 * @return	자신 빌더 객체.
		 */
		public Builder partitionDimension(Size2i dim) {
			m_partDim = dim;
			return this;
		}
		
		/**
		 * 생성될 전체 grid 셀의 갯수를 설정한다.
		 * 
		 * @param count	셀의 갯수
		 * @return	자신 빌더 객체.
		 */
		public Builder partitionCount(int count) {
			int side = (int)Math.ceil(Math.sqrt(count));
			m_partDim = new Size2i(side, side);
			return this;
		}
	}
}
