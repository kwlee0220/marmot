package marmot.remote.protobuf;

import java.io.InputStream;

import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.stub.StreamObserver;
import marmot.BindDataSetOptions;
import marmot.MarmotCore;
import marmot.MarmotInternalException;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.DataSet;
import marmot.dataset.DataSetImpl;
import marmot.dataset.DataSetType;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.geo.catalog.SpatialIndexInfo;
import marmot.geo.command.ClusterSpatiallyOptions;
import marmot.geo.command.CreateSpatialIndexOptions;
import marmot.geo.command.EstimateQuadKeysOptions;
import marmot.geo.query.RangeQueryEstimate;
import marmot.optor.CreateDataSetOptions;
import marmot.proto.StringProto;
import marmot.proto.VoidProto;
import marmot.proto.service.AppendRecordSetRequest;
import marmot.proto.service.BindDataSetRequest;
import marmot.proto.service.BoolResponse;
import marmot.proto.service.BuildDataSetRequest;
import marmot.proto.service.ClusterSpatiallyRequest;
import marmot.proto.service.CreateDataSetRequest;
import marmot.proto.service.CreateSpatialIndexRequest;
import marmot.proto.service.CreateThumbnailRequest;
import marmot.proto.service.DataSetInfoResponse;
import marmot.proto.service.DataSetServiceGrpc.DataSetServiceImplBase;
import marmot.proto.service.DirectoryTraverseRequest;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.EstimateQuadKeysRequest;
import marmot.proto.service.EstimateRangeQueryRequest;
import marmot.proto.service.FloatResponse;
import marmot.proto.service.LongResponse;
import marmot.proto.service.MoveDataSetRequest;
import marmot.proto.service.MoveDirRequest;
import marmot.proto.service.QueryRangeRequest;
import marmot.proto.service.RangeQueryEstimateResponse;
import marmot.proto.service.ReadDataSetRequest;
import marmot.proto.service.ReadRawSpatialClusterRequest;
import marmot.proto.service.ReadThumbnailRequest;
import marmot.proto.service.SpatialIndexInfoResponse;
import marmot.proto.service.StringResponse;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.proto.service.UpdateGeometryColumnInfoRequest;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBRecordProtos;
import marmot.protobuf.PBUtils;
import utils.Throwables;
import utils.func.FOption;
import utils.io.Lz4Compressions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBDataSetServiceServant<T extends MarmotCore> extends DataSetServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(PBDataSetServiceServant.class);
	
	protected final T m_marmot;
	
	public PBDataSetServiceServant(T marmot) {
		m_marmot = marmot;
	}
	
	@Override
	public void createDataSet(CreateDataSetRequest req, StreamObserver<DataSetInfoResponse> resp) {
		try {
			String dsId = req.getId();
			CreateDataSetOptions opts = CreateDataSetOptions.fromProto(req.getOptions());
			
			DataSetImpl ds;
			switch ( req.getEitherInitializationCase() ) {
				case RECORD_SCHEMA:
					RecordSchema schema = RecordSchema.fromProto(req.getRecordSchema());
					s_logger.debug("createDataSet: dataset={}, schema={}", dsId, schema);
					ds = m_marmot.createDataSet(dsId, schema, opts);
					break;
//				case PLAN_EXEC:
//					ds = createDataSetAndExecute(dsId, opts, req.getPlanExec());
//					break;
				default:
					throw new AssertionError();
			}
			
			resp.onNext(toResponse(ds));
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			resp.onNext(toResponse(cause));
		}
		resp.onCompleted();
	}
	
//	private DataSetImpl createDataSetAndExecute(String dsId, CreateDataSetOptions dsOpts,
//												ExecutePlanRequest req) {			
//		Plan plan = Plan.fromProto(req.getPlan());
//		ExecutePlanOptions execOpts = ExecutePlanOptions.fromProto(req.getOptions());
//		
//		// Plan 수행 결과 DataSet의 Schema를 계산하여 empty 데이터세트를 생성한다.
//		RecordSchema outSchema = m_marmot.getOutputRecordSchema(plan);
//		m_marmot.createDataSet(dsId, outSchema, dsOpts);
//		
//		try {
//			m_marmot.execute(plan, execOpts);
//			return m_marmot.getDataSet(dsId);
//		}
//		catch ( Exception e ) {
//			Try.run(() -> m_marmot.deleteDataSet(dsId));
//			throw e;
//		}
//	}
	
	@Override
    public void bindDataSet(BindDataSetRequest req,
    						StreamObserver<DataSetInfoResponse> resp) {
		try {
			String dsId = req.getDataset();
			String path = req.getFilePath();
			DataSetType type = DataSetType.fromString(req.getType().name());
			BindDataSetOptions opts = BindDataSetOptions.fromProto(req.getOptions());

			s_logger.debug("bindDataSet: dataset={}, path={}", dsId, path);
			DataSetImpl ds = m_marmot.bindExternalDataSet(dsId, path, type, opts);

			resp.onNext(toResponse(ds));
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			resp.onNext(toResponse(cause));
		}
		resp.onCompleted();
	}
	
	@Override
    public void buildDataSet(BuildDataSetRequest req,
    						StreamObserver<DataSetInfoResponse> resp) {
		try {
			String dsId = req.getDataset();
			String path = req.getFilePath();
			String infoPath = req.getInfoFilePath();
			BindDataSetOptions opts = BindDataSetOptions.fromProto(req.getOptions());

			s_logger.debug("buildDataSet: dataset={}, path={}, infoPath={}", dsId, path, infoPath);
			DataSetImpl ds = m_marmot.buildDataSet(dsId, path, infoPath, opts);

			resp.onNext(toResponse(ds));
		}
		catch ( Exception e ) {
			Throwable cause = Throwables.unwrapThrowable(e);
			resp.onNext(toResponse(cause));
		}
		resp.onCompleted();
	}
	
	@Override
    public void deleteDataSet(StringProto req, StreamObserver<BoolResponse> response) {
		s_logger.debug("deleteDataSet: dataset={}", req.getValue());
		PBUtils.replyBoolean(() -> m_marmot.deleteDataSet(req.getValue()), response);
	}

	@Override
    public void moveDataSet(MoveDataSetRequest req, StreamObserver<VoidResponse> observer) {
		try {
			s_logger.debug("moveDataSet: from={}, to={}", req.getSrcId(), req.getDestId());
			m_marmot.moveDataSet(req.getSrcId(), req.getDestId());
			observer.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			observer.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			observer.onCompleted();
		}
    }

	@Override
    public StreamObserver<DownChunkResponse> readDataSet(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws InvalidProtocolBufferException {
				ReadDataSetRequest req = ReadDataSetRequest.parseFrom(header);
				
				try {
					DataSet ds = m_marmot.getDataSet(req.getId());

					InputStream rsetStream = PBRecordProtos.toInputStream(ds.read());
					if ( req.getUseCompression() ) {
						rsetStream = Lz4Compressions.compress(rsetStream);
					}
					s_logger.debug("readDataSet: dataset={}", req.getId());
					
					return rsetStream;
				}
				catch ( Exception e ) {
					throw new MarmotInternalException(e);
				}
			}
		};
		sender.start();
		
		return sender;
	}
	
	@Override
    public StreamObserver<UpChunkRequest>
	appendRecordSet(StreamObserver<UpChunkResponse> channel) {
		return new StreamUploadReceiver(channel) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is)
				throws Exception {
				AppendRecordSetRequest req = AppendRecordSetRequest.parseFrom(header);

				DataSet ds = m_marmot.getDataSet(req.getId());
				if ( req.getUseCompression() ) {
					is = Lz4Compressions.decompress(is);
				}
				RecordSet rset = PBRecordProtos.readRecordSet(is);
				s_logger.debug("appendRecordSet: dataset={}...", req.getId());
				
				String partId = null;
				switch ( req.getOptionalPartitionIdCase() ) {
					case PARTITION_ID:
						partId = req.getPartitionId();
						break;
					default:
						break;
				}
				
				long count = (partId != null) ? ds.append(rset, partId) : ds.append(rset);
				return PBUtils.toLongResponse(count).toByteString();
			}
		};
	}
	
	@Override
    public void getDataSetLength(StringProto req, StreamObserver<LongResponse> resp) {
		try {
			DataSet ds = m_marmot.getDataSet(req.getValue());
			long length = ds.length();

			resp.onNext(LongResponse.newBuilder()
										.setValue(length)
										.build());
		}
		catch ( Exception e ) {
			resp.onNext(LongResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getDefaultSpatialIndexInfo(StringProto request,
    									StreamObserver<SpatialIndexInfoResponse> resp) {
		try {
			String dsId = PBUtils.fromProto(request);
			DataSet ds = m_marmot.getDataSet(dsId);
			
			SpatialIndexInfoResponse infoResp
						= ds.getSpatialIndexInfo()
							.map(info -> SpatialIndexInfoResponse.newBuilder()
												.setIndexInfo(info.toProto())
												.build())
							.getOrElse(() ->  SpatialIndexInfoResponse.newBuilder()
												.setNone(PBUtils.VOID)
												.build());
			resp.onNext(infoResp);
		}
		catch ( Exception e ) {
			resp.onNext(SpatialIndexInfoResponse.newBuilder()
													.setError(PBUtils.toErrorProto(e))
													.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void createSpatialIndex(CreateSpatialIndexRequest req,
    								StreamObserver<SpatialIndexInfoResponse> resp) {
		try {
			String dsId = req.getId();
			DataSet ds = m_marmot.getDataSet(dsId);
			CreateSpatialIndexOptions opts = CreateSpatialIndexOptions.fromProto(req.getOptions());

			s_logger.debug("clusterDataSet: dataset={}", dsId);
			SpatialIndexInfo info = ds.createSpatialIndex(opts);
			resp.onNext(SpatialIndexInfoResponse.newBuilder()
													.setIndexInfo(info.toProto())
													.build());
		}
		catch ( Exception e ) {
			resp.onNext(SpatialIndexInfoResponse.newBuilder()
													.setError(PBUtils.toErrorProto(e))
													.build());
		}
		finally {
			resp.onCompleted();
		}
	}

	@Override
    public void deleteSpatialIndex(StringProto req,
    								StreamObserver<VoidResponse> resp) {
		try {
			String dsId = PBUtils.fromProto(req);
			DataSet ds = m_marmot.getDataSet(dsId);

			s_logger.debug("deleteSpatialCluster: dataset={}", dsId);
			ds.deleteSpatialIndex();
			
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfo(StringProto req, StreamObserver<DataSetInfoResponse> resp) {
		try {
			DataSetImpl ds = m_marmot.getDataSet(PBUtils.fromProto(req));
			DataSetInfo info = ds.getDataSetInfo();
			resp.onNext(DataSetInfoResponse.newBuilder()
												.setDatasetInfo(info.toProto())
												.build());
		}
		catch ( Exception e ) {
			resp.onNext(DataSetInfoResponse.newBuilder()
												.setError(PBUtils.toErrorProto(e))
												.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfoAll(VoidProto req, StreamObserver<DataSetInfoResponse> resp) {
		try {
			m_marmot.getDataSetAll()
					.stream()
					.map(ds -> ((DataSetImpl)ds).getDataSetInfo())
					.map(info -> DataSetInfoResponse.newBuilder()
													.setDatasetInfo(info.toProto())
													.build())
					.forEach(proto -> resp.onNext(proto));
		}
		catch ( Exception e ) {
			resp.onNext(DataSetInfoResponse.newBuilder()
					.setError(PBUtils.toErrorProto(e))
					.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getDataSetInfoAllInDir(DirectoryTraverseRequest req,
    								StreamObserver<DataSetInfoResponse> resp) {
		try {
			m_marmot.getDataSetAllInDir(req.getDirectory(), req.getRecursive())
					.stream()
					.map(ds -> ((DataSetImpl)ds).getDataSetInfo())
					.map(info -> DataSetInfoResponse.newBuilder()
													.setDatasetInfo(info.toProto())
													.build())
					.forEach(proto -> resp.onNext(proto));
		}
		catch ( Exception e ) {
			resp.onNext(DataSetInfoResponse.newBuilder()
					.setError(PBUtils.toErrorProto(e))
					.build());
		}
		finally {
			resp.onCompleted();
		}
	}

	@Override
    public void updateGeometryColumnInfo(UpdateGeometryColumnInfoRequest req,
            							StreamObserver<DataSetInfoResponse> resp) {
		try {
			FOption<GeometryColumnInfo> newGcInfo;
			switch ( req.getOptionalGeometryColumnInfoCase() ) {
				case GC_INFO:
					GeometryColumnInfo info = GeometryColumnInfo.fromProto(req.getGcInfo());
					newGcInfo = FOption.of(info);
					break;
				case OPTIONALGEOMETRYCOLUMNINFO_NOT_SET:
					newGcInfo = FOption.empty();
					break;
				default:
					throw new AssertionError();
			}
			
			DataSetImpl ds = m_marmot.getDataSet(req.getId());
			ds.updateGeometryColumnInfo(newGcInfo);
			
			DataSetInfo dsInfo = ds.getDataSetInfo();
			resp.onNext(DataSetInfoResponse.newBuilder()
												.setDatasetInfo(dsInfo.toProto())
												.build());
		}
		catch ( Exception e ) {
			resp.onNext(DataSetInfoResponse.newBuilder()
					.setError(PBUtils.toErrorProto(e))
					.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getDirAll(VoidProto req, StreamObserver<StringResponse> resp) {
		try {
			m_marmot.getDirAll()
					.stream()
					.map(PBUtils::toStringResponse)
					.forEach(resp::onNext);
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toStringResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getSubDirAll(DirectoryTraverseRequest req, StreamObserver<StringResponse> resp) {
		try {
			m_marmot.getSubDirAll(req.getDirectory(), req.getRecursive())
					.stream()
					.map(PBUtils::toStringResponse)
					.forEach(resp::onNext);
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toStringResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getParentDir(StringProto req, StreamObserver<StringResponse> resp) {
		try {
			String dir = m_marmot.getParentDir(req.getValue());
			resp.onNext(PBUtils.toStringResponse(dir));
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toStringResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void moveDir(MoveDirRequest req, StreamObserver<VoidResponse> resp) {
		try {
			String srcPath = req.getSrcPath();
			String tarPath = req.getDestPath();

			s_logger.debug("moveDir: from={} to={}", srcPath,  tarPath);
			m_marmot.moveDir(srcPath, tarPath);
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void deleteDir(StringProto req, StreamObserver<VoidResponse> resp) {
		try {
			s_logger.debug("deleteDir: dir={}", req.getValue());
			m_marmot.deleteDir(req.getValue());
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void estimateRangeQuery(EstimateRangeQueryRequest req,
    								StreamObserver<RangeQueryEstimateResponse> channel) {
		try {
			String dsId = req.getDatasetId();
			Envelope range = PBUtils.fromProto(req.getRange());
			s_logger.debug("estimateRangeQuery: ds={}. range={}", dsId, range);
			
			DataSet ds = m_marmot.getDataSet(dsId);
			RangeQueryEstimate est = ds.estimateRangeQuery(range);
			RangeQueryEstimateResponse resp = RangeQueryEstimateResponse.newBuilder()
																		.setEstimate(est.toProto())
																		.build();
			channel.onNext(resp);
		}
		catch ( Exception e ) {
			RangeQueryEstimateResponse resp = RangeQueryEstimateResponse.newBuilder()
																	.setError(PBUtils.toErrorProto(e))
																	.build();
			channel.onNext(resp);
		}
		finally {
			channel.onCompleted();
		}
	}
	
	@Override
    public StreamObserver<DownChunkResponse> queryRange(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				QueryRangeRequest req = QueryRangeRequest.parseFrom(header);
				
				DataSet ds = m_marmot.getDataSet(req.getId());
				Envelope range = PBUtils.fromProto(req.getRange());
				int sampleCount = req.getSampleCount();
		
				if ( s_logger.isInfoEnabled() ) {
					s_logger.debug("queryRange: dataset={}, range={}, nsamples={}",
									ds.getId(), range, sampleCount);
				}
				RecordSet rset = ds.queryRange(range, sampleCount);
				InputStream in = PBRecordProtos.toInputStream(rset);
				if ( req.getUseCompression() ) {
					in = Lz4Compressions.compress(in);
				}
				return in;
			}
		};
		handler.start();
		
		return handler;
    }
	
	@Override
    public StreamObserver<DownChunkResponse>
	readRawSpatialCluster(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				ReadRawSpatialClusterRequest req = ReadRawSpatialClusterRequest.parseFrom(header);

				DataSetImpl ds = m_marmot.getDataSet(req.getDatasetId());
				String quadKey = req.getQuadKey();

				s_logger.debug("readRawSpatialCluster: dataset={}, quad-key={}",
								ds.getId(), quadKey);
				RecordSet cluster = ds.readSpatialCluster(quadKey);
				InputStream is = PBRecordProtos.toInputStream(cluster);
				if ( req.getUseCompression() ) {
					is = Lz4Compressions.compress(is);
				}
				return is;
			}
		};
		handler.start();
		
		return handler;
    }
	
	@Override
    public void getClusterQuadKeyAll(StringProto req, StreamObserver<StringResponse> resp) {
		try {
			String dsId = req.getValue();
			DataSet ds = m_marmot.getDataSet(dsId);
			
			ds.getClusterQuadKeyAll()
				.stream()
				.map(PBUtils::toStringResponse)
				.forEach(resp::onNext);
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toStringResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}

	@Override
    public void estimateQuadKeys(EstimateQuadKeysRequest req, StreamObserver<StringResponse> resp) {
		try {
			String dsId = req.getDsId();
			DataSet ds = m_marmot.getDataSet(dsId);
			EstimateQuadKeysOptions opts = EstimateQuadKeysOptions.fromProto(req.getOptions());
			
			ds.estimateQuadKeys(opts)
				.stream()
				.map(PBUtils::toStringResponse)
				.forEach(resp::onNext);
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toStringResponse(e));
		}
		finally {
			resp.onCompleted();
		}
    }

	@Override
    public void clusterSpatially(ClusterSpatiallyRequest req, StreamObserver<VoidResponse> resp) {
		try {
			String dsId = req.getDsId();
			DataSet ds = m_marmot.getDataSet(dsId);

			String outDsId;
			switch ( req.getOptionalOutDsIdCase() ) {
				case OUT_DS_ID:
					outDsId = req.getOutDsId();
					break;
				case OPTIONALOUTDSID_NOT_SET:
					outDsId = null;
					break;
				default:
					throw new AssertionError();
			}
			
			ClusterSpatiallyOptions opts = ClusterSpatiallyOptions.fromProto(req.getOptions());
			ds.cluster(outDsId, opts);
			
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void getBlockSize(StringProto req,  StreamObserver<LongResponse> resp) {
		try {
			DataSet ds = m_marmot.getDataSet(req.getValue());
			long blkSize = ds.getBlockSize();

			resp.onNext(LongResponse.newBuilder()
										.setValue(blkSize)
										.build());
		}
		catch ( Exception e ) {
			resp.onNext(LongResponse.newBuilder()
					.setError(PBUtils.toErrorProto(e))
					.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void createThumbnail(CreateThumbnailRequest req,
            					StreamObserver<VoidResponse> resp) {
		try {
			String dsId = req.getId();
			DataSet ds = m_marmot.getDataSet(dsId);
			
			ds.createThumbnail(req.getCount());
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void deleteThumbnail(StringProto req, StreamObserver<BoolResponse> resp) {
		try {
			String dsId = PBUtils.fromProto(req);
			DataSet ds = m_marmot.getDataSet(dsId);
			
			resp.onNext(PBUtils.toBoolResponse(ds.deleteThumbnail()));
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toBoolResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public StreamObserver<DownChunkResponse> readThumbnail(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				ReadThumbnailRequest req = ReadThumbnailRequest.parseFrom(header);
				
				DataSetImpl ds = (DataSetImpl)m_marmot.getDataSet(req.getId());
				Envelope range = PBUtils.fromProto(req.getRange());
				int count = req.getCount();
		
				s_logger.debug("read_thumbnail: dataset={}, range={} count={}",
								ds.getId(), range, count);
				RecordSet rset = ds.readThumbnail(range, count);
				InputStream is = PBRecordProtos.toInputStream(rset);
				if ( req.getUseCompression() ) {
					is = Lz4Compressions.compress(is);
				}
				return is;
			}
		};
		handler.start();
		
		return handler;
    }
	
	@Override
    public void getThumbnailRatio(StringProto req, StreamObserver<FloatResponse> resp) {
		try {
			String dsId = PBUtils.fromProto(req);
			DataSetImpl ds = m_marmot.getDataSet(dsId);
			
			resp.onNext(PBUtils.toFloatResponse(ds.getThumbnailRatio().getOrElse(-1f)));
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toFloatResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	private DataSetInfoResponse toResponse(DataSetImpl ds) {
		return DataSetInfoResponse.newBuilder()
								.setDatasetInfo(ds.getDataSetInfo().toProto())
								.build();
	}
	
	private DataSetInfoResponse toResponse(Throwable cause) {
		return DataSetInfoResponse.newBuilder()
								.setError(PBUtils.toErrorProto(cause))
								.build();
	}
}
