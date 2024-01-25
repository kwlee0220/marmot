package marmot.remote.protobuf;

import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import marmot.ExecutePlanOptions;
import marmot.MarmotRuntime;
import marmot.Plan;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.analysis.system.SystemAnalysis;
import marmot.exec.CompositeAnalysis;
import marmot.exec.ExecutionNotFoundException;
import marmot.exec.MarmotAnalysis;
import marmot.exec.MarmotExecution;
import marmot.exec.MarmotExecution.State;
import marmot.module.MarmotModuleRegistry;
import marmot.proto.StringProto;
import marmot.proto.VoidProto;
import marmot.proto.service.AddAnalysisRequest;
import marmot.proto.service.BoolResponse;
import marmot.proto.service.DeleteAnalysisRequest;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.ExecutePlanRequest;
import marmot.proto.service.ExecuteProcessRequest;
import marmot.proto.service.ExecutionInfoListResponse;
import marmot.proto.service.ExecutionInfoListResponse.ExecutionInfoListProto;
import marmot.proto.service.ExecutionInfoProto;
import marmot.proto.service.ExecutionInfoProto.ExecutionStateInfoProto;
import marmot.proto.service.ExecutionInfoProto.ExecutionStateProto;
import marmot.proto.service.ExecutionInfoResponse;
import marmot.proto.service.GetOutputRecordSchemaRequest;
import marmot.proto.service.GetStreamRequest;
import marmot.proto.service.MarmotAnalysisProto;
import marmot.proto.service.MarmotAnalysisResponse;
import marmot.proto.service.OptionalRecordResponse;
import marmot.proto.service.PlanExecutionServiceGrpc.PlanExecutionServiceImplBase;
import marmot.proto.service.RecordSchemaResponse;
import marmot.proto.service.SetExecutionInfoRequest;
import marmot.proto.service.StringListResponse;
import marmot.proto.service.TimeoutProto;
import marmot.proto.service.UpRequestDownResponse;
import marmot.proto.service.UpResponseDownRequest;
import marmot.proto.service.VoidResponse;
import marmot.proto.service.WaitForFinishedRequest;
import marmot.protobuf.PBRecordProtos;
import marmot.protobuf.PBUtils;
import marmot.protobuf.PBValueProtos;
import marmot.remote.protobuf.StreamObservers.ServerUpDownChannel;
import utils.io.Lz4Compressions;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBPlanExecutionServiceServant extends PlanExecutionServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(PBPlanExecutionServiceServant.class);
	
	protected final MarmotRuntime m_runtime;
	
	public PBPlanExecutionServiceServant(MarmotRuntime marmot) {
		m_runtime = marmot;
	}
	
	public void setGrpcServer(Server server) {
	}
	
	@Override
    public void getOutputRecordSchema(GetOutputRecordSchemaRequest req,
    									StreamObserver<RecordSchemaResponse> resp) {
		try {
			Plan plan = Plan.fromProto(req.getPlan());
			
			RecordSchema outSchema;
			switch ( req.getOptionalInputSchemaCase() ) {
				case INPUT_SCHEMA:
					RecordSchema inSchema = RecordSchema.fromProto(req.getInputSchema());
					outSchema = m_runtime.getOutputRecordSchema(plan, inSchema);
					break;
				case OPTIONALINPUTSCHEMA_NOT_SET:
					outSchema = m_runtime.getOutputRecordSchema(plan);
					break;
				default:
					throw new AssertionError();
			}
			resp.onNext(RecordSchemaResponse.newBuilder()
												.setRecordSchema(outSchema.toProto())
												.build());
		}
		catch ( Exception e ) {
			resp.onNext(RecordSchemaResponse.newBuilder()
												.setError(PBUtils.toErrorProto(e))
												.build());
		}
		finally {
			resp.onCompleted();
		}	
    }

    public void getModuleAnalysisClassIdAll(VoidProto req,
    										StreamObserver<StringListResponse> channel) {
		try {
			Set<String> idSet = MarmotModuleRegistry.get().getModuleAnalysisClassAll();
			channel.onNext(PBUtils.toStringListResponse(idSet));
		}
		catch ( Exception e ) {
			channel.onNext(PBUtils.toStringListResponse(e));
		}
		finally {
			channel.onCompleted();
		}
    }

    public void getModuleAnalysisParameterNameAll(StringProto req,
    												StreamObserver<StringListResponse> channel) {
		try {
			List<String> params = MarmotModuleRegistry.get().getModuleParameterNameAll(req.getValue());
			channel.onNext(PBUtils.toStringListResponse(params));
		}
		catch ( Exception e ) {
			channel.onNext(PBUtils.toStringListResponse(e));
		}
		finally {
			channel.onCompleted();
		}
    }

    public void getSystemAnalysisClassIdAll(VoidProto req,
    										StreamObserver<StringListResponse> channel) {
		try {
			Set<String> idSet = SystemAnalysis.getSystemAnalysisClassIdAll();
			channel.onNext(PBUtils.toStringListResponse(idSet));
		}
		catch ( Exception e ) {
			channel.onNext(PBUtils.toStringListResponse(e));
		}
		finally {
			channel.onCompleted();
		}
    }

    public void getSystemAnalysisParameterNameAll(StringProto req,
    												StreamObserver<StringListResponse> channel) {
		try {
			List<String> params = SystemAnalysis.getSystemParameterNameAll(req.getValue());
			channel.onNext(PBUtils.toStringListResponse(params));
		}
		catch ( Exception e ) {
			channel.onNext(PBUtils.toStringListResponse(e));
		}
		finally {
			channel.onCompleted();
		}
    }

	@Override
	public void findAnalysis(StringProto req, StreamObserver<MarmotAnalysisResponse> respOb) {
		try {
			MarmotAnalysis analysis = m_runtime.findAnalysis(req.getValue());
			if ( analysis != null ) {
				respOb.onNext(toMarmotAnalysisResponse(analysis));
			}
			else {
				respOb.onNext(MarmotAnalysisResponse.newBuilder().build());
			}
		}
		catch ( Exception e ) {
			respOb.onNext(toMarmotAnalysisResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
	public void findParentAnalysis(StringProto req, StreamObserver<MarmotAnalysisResponse> respOb) {
		try {
			CompositeAnalysis analysis = m_runtime.findParentAnalysis(req.getValue());
			if ( analysis != null ) {
				respOb.onNext(toMarmotAnalysisResponse(analysis));
			}
			else {
				respOb.onNext(MarmotAnalysisResponse.newBuilder().build());
			}
		}
		catch ( Exception e ) {
			respOb.onNext(toMarmotAnalysisResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
    public void getAncestorAnalysisAll(StringProto req, StreamObserver<MarmotAnalysisResponse> respOb) {
		try {
			m_runtime.getAncestorAnalysisAll(req.getValue())
					.stream()
					.map(this::toMarmotAnalysisResponse)
					.forEach(respOb::onNext);
		}
		catch ( Exception e ) {
			respOb.onNext(toMarmotAnalysisResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
	public void getDescendantAnalysisAll(StringProto req, StreamObserver<MarmotAnalysisResponse> respOb) {
		try {
			m_runtime.getDescendantAnalysisAll(req.getValue())
					.stream()
					.map(this::toMarmotAnalysisResponse)
					.forEach(respOb::onNext);
		}
		catch ( Exception e ) {
			respOb.onNext(toMarmotAnalysisResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
	public void getAnalysisAll(VoidProto req, StreamObserver<MarmotAnalysisResponse> respOb) {
		try {
			m_runtime.getAnalysisAll()
					.stream()
					.map(this::toMarmotAnalysisResponse)
					.forEach(respOb::onNext);
		}
		catch ( Exception e ) {
			respOb.onNext(toMarmotAnalysisResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
    public void addAnalysis(AddAnalysisRequest req, StreamObserver<VoidResponse> respOb) {
		try {
			MarmotAnalysis analysis = MarmotAnalysis.fromProto(req.getAnalysis());
			m_runtime.addAnalysis(analysis, req.getForce());
			respOb.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			respOb.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			respOb.onCompleted();
		}	
	}

	@Override
    public void deleteAnalysis(DeleteAnalysisRequest req,  StreamObserver<VoidResponse> respOb) {
		try {
			m_runtime.deleteAnalysis(req.getId(), req.getRecursive());
			respOb.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			respOb.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			respOb.onCompleted();
		}	
    }

	@Override
    public void deleteAnalysisAll(VoidProto req,  StreamObserver<VoidResponse> respOb) {
		try {
			m_runtime.deleteAnalysisAll();
			respOb.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			respOb.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			respOb.onCompleted();
		}	
    }

	@Override
    public void startAnalysis(MarmotAnalysisProto req, StreamObserver<ExecutionInfoResponse> respOb) {
		try {
			MarmotAnalysis analysis = MarmotAnalysis.fromProto(req);
			MarmotExecution exec = m_runtime.startAnalysis(analysis);

			respOb.onNext(toExecutionInfoResponse(exec, null));
		}
		catch ( Exception e ) {
			respOb.onNext(toExecutionInfoResponse(null, e));
		}
		finally {
			respOb.onCompleted();
		}
	}

	@Override
    public void executeAnalysis(MarmotAnalysisProto req,  StreamObserver<VoidResponse> respOb) {
		try {
			MarmotAnalysis analysis = MarmotAnalysis.fromProto(req);
			m_runtime.executeAnalysis(analysis);
			respOb.onNext(PBUtils.toVoidResponse());
		}
		catch ( Exception e ) {
			respOb.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			respOb.onCompleted();
		}
	}
	
	@Override
    public void start(ExecutePlanRequest req, StreamObserver<ExecutionInfoResponse> respOb) {
		try {
			Plan plan = Plan.fromProto(req.getPlan());
			ExecutePlanOptions opts = ExecutePlanOptions.fromProto(req.getOptions());
			MarmotExecution exec = m_runtime.start(plan, opts);

			respOb.onNext(toExecutionInfoResponse(exec, null));
		}
		catch ( Exception e ) {
			respOb.onNext(toExecutionInfoResponse(null, e));
		}
		finally {
			respOb.onCompleted();
		}	
    }

	@Override
    public void getExecutionInfo(StringProto req, StreamObserver<ExecutionInfoResponse> respOb) {
		String execId = req.getValue();
		try {
			MarmotExecution exec = m_runtime.getMarmotExecution(execId);
			if ( exec != null ) {
				respOb.onNext(toExecutionInfoResponse(exec, null));
			}
			else {
				Throwable error = new ExecutionNotFoundException("id=" + execId);
				respOb.onNext(toExecutionInfoResponse(null, error));
			}
		}
		catch ( Exception e ) {
			respOb.onNext(toExecutionInfoResponse(null, e));
		}
		finally {
			respOb.onCompleted();
		}
	}

    public void getExecutionInfoList(VoidProto req,
    								StreamObserver<ExecutionInfoListResponse> respOb) {
		try {
			List<ExecutionInfoProto> infoList = FStream.from(m_runtime.getMarmotExecutionAll())
														.map(this::toExecutionInfoProto)
														.toList();
			ExecutionInfoListProto listProto = ExecutionInfoListProto.newBuilder()
																	.addAllExecInfo(infoList)
																	.build();
			ExecutionInfoListResponse resp = ExecutionInfoListResponse.newBuilder()
																		.setExecInfoList(listProto)
																		.build();
			respOb.onNext(resp);
		}
		catch ( Exception e ) {
			respOb.onNext(ExecutionInfoListResponse.newBuilder()
													.setError(PBUtils.toErrorProto(e))
													.build());
		}
		finally {
			respOb.onCompleted();
		}
    }
	
	@Override
    public void setExecutionInfo(SetExecutionInfoRequest req,
    							StreamObserver<ExecutionInfoResponse> respOb) {
		String execId = req.getExecId();
		MarmotExecution exec = m_runtime.getMarmotExecution(execId);
		
		try {	
			if ( exec != null ) {
				ExecutionInfoProto info = req.getExecInfo();
				exec.setMaximumRunningTime(Duration.ofMillis(info.getMaxRunningTime()));
				exec.setRetentionTime(Duration.ofMillis(info.getRetentionTime()));
				respOb.onNext(toExecutionInfoResponse(exec, null));
			}
			else {
				Throwable error = new ExecutionNotFoundException("id=" + execId);
				respOb.onNext(toExecutionInfoResponse(null, error));
			}
		}
		catch ( Exception e ) {
			respOb.onNext(toExecutionInfoResponse(null, e));
		}
		finally {
			respOb.onCompleted();
		}
	}
	
	@Override
    public void cancelExecution(StringProto req, StreamObserver<BoolResponse> resp) {
		try {
			MarmotExecution exec = m_runtime.getMarmotExecution(req.getValue());
			
			boolean result = false;
			if ( exec != null ) {
				result = exec.cancel();
			}
			
			resp.onNext(PBUtils.toBoolResponse(result));
		}
		catch ( Exception e ) {
			resp.onNext(PBUtils.toBoolResponse(e));
		}
		finally {
			resp.onCompleted();
		}	
	}
	
	@Override
    public void waitForFinished(WaitForFinishedRequest req,
    							StreamObserver<ExecutionInfoResponse> respOb) {
		MarmotExecution exec = m_runtime.getMarmotExecution(req.getExecId());
		
		try {	
			if ( exec != null ) {
				switch ( req.getOptionalTimeoutCase() ) {
					case TIMEOUT:
						TimeoutProto timeout = req.getTimeout();
						exec.waitForFinished(timeout.getTimeout(),
											TimeUnit.valueOf(timeout.getTimeUnit()));
						break;
					case OPTIONALTIMEOUT_NOT_SET:
						exec.waitForFinished();
						break;
				}
				
				respOb.onNext(toExecutionInfoResponse(exec, null));
			}
			else {
				Throwable error = new ExecutionNotFoundException("id=" + req.getExecId());
				respOb.onNext(toExecutionInfoResponse(null, error));
			}
		}
		catch ( Exception e ) {
			respOb.onNext(toExecutionInfoResponse(null, e));
		}
		finally {
			respOb.onCompleted();
		}	
	}
	
	private ExecutionInfoProto toExecutionInfoProto(MarmotExecution exec) {
		ExecutionStateProto stateProto = toExecutionStateProto(exec.getState());
		ExecutionStateInfoProto.Builder sbuilder = ExecutionStateInfoProto.newBuilder()
																		.setState(stateProto);
		if ( exec.getState() == State.FAILED ) {
			sbuilder = sbuilder.setFailureCause(PBUtils.toErrorProto(exec.getFailureCause()));
		}
		ExecutionStateInfoProto stateInfoProto = sbuilder.build();
		
		int current = exec.getCurrentExecutionIndex();
		ExecutionInfoProto.Builder builder = ExecutionInfoProto.newBuilder()
												.setId(exec.getId())
												.setStartedTime(exec.getStartedTime())
												.setFinishedTime(exec.getFinishedTime())
												.setMaxRunningTime(exec.getMaximumRunningTime().toMillis())
												.setRetentionTime(exec.getRetentionTime().toMillis())
												.setExecStateInfo(stateInfoProto)
												.setCurrentExecIndex(current);
		builder = exec.getMarmotAnalysis().transform(builder, (b,a) -> b.setAnalysisId(a.getId()));
		return builder.build();
	}
	
	private ExecutionInfoResponse toExecutionInfoResponse(MarmotExecution exec, Throwable error) {
		if ( exec != null ) {
			return ExecutionInfoResponse.newBuilder().setExecInfo(toExecutionInfoProto(exec)).build();
		}
		else {
			return ExecutionInfoResponse.newBuilder().setError(PBUtils.toErrorProto(error)).build();
		}
	}
	
	@Override
    public void execute(ExecutePlanRequest req, StreamObserver<VoidResponse> resp) {
		try {
			Plan plan = Plan.fromProto(req.getPlan());
			ExecutePlanOptions opts = ExecutePlanOptions.fromProto(req.getOptions());
			m_runtime.execute(plan, opts);
			
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
    public StreamObserver<DownChunkResponse>
	executeLocally(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				ExecutePlanRequest req = ExecutePlanRequest.parseFrom(header);
				
				Plan plan = Plan.fromProto(req.getPlan());
				s_logger.debug("executeLocally");
				RecordSet rset = m_runtime.executeLocally(plan);
				
				InputStream rsetStream = PBRecordProtos.toInputStream(rset);
				if ( req.getUseCompression() ) {
					rsetStream = Lz4Compressions.compress(rsetStream);
				}
				return rsetStream;
			}
		};
		handler.start();
		
		return handler;
	}
	
	@Override
    public StreamObserver<UpRequestDownResponse>
	executeLocallyWithInput(StreamObserver<UpResponseDownRequest> req) {
		ServerUpDownChannel channel = StreamObservers.getServerUpDownChannel(req);
		
		StreamDownloadSender downloader = new StreamDownloadSender(channel.getDownloadChannel());
		StreamUploadReceiver uploader = new StreamUploadReceiver(channel.getUploadChannel()) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is)
				throws Exception {
				ExecutePlanRequest execReq = ExecutePlanRequest.parseFrom(header);

				Plan plan = Plan.fromProto(execReq.getPlan());
				
				RecordSet output;
				if ( execReq.getHasInputRset() ) {
					RecordSet input = PBRecordProtos.readRecordSet(is);
					output = m_runtime.executeLocally(plan, input);
				}
				else {
					output = m_runtime.executeLocally(plan);
				}
				
				InputStream rsetStream = PBRecordProtos.toInputStream(output);
				if ( execReq.getUseCompression() ) {
					rsetStream = Lz4Compressions.compress(rsetStream);
				}
				downloader.setInputStream(rsetStream);
				downloader.start();
				
				return null;
			}
		};
		
		return new StreamUpnDownloadServer(uploader, downloader);
	}
	
	@Override
    public void executeToSingle(ExecutePlanRequest req, StreamObserver<OptionalRecordResponse> resp) {
		try {
			Plan plan = Plan.fromProto(req.getPlan());
			
			m_runtime.executeToRecord(plan)
					.ifPresent(r -> resp.onNext(OptionalRecordResponse.newBuilder()
																	.setRecord(PBRecordProtos.toProto(r))
																	.build()))
					.ifAbsent(() -> resp.onNext(OptionalRecordResponse.newBuilder()
																	.setNone(PBUtils.VOID)
																	.build()));
		}
		catch ( Exception e ) {
			resp.onNext(OptionalRecordResponse.newBuilder()
												.setError(PBUtils.toErrorProto(e))
												.build());
		}
		finally {
			resp.onCompleted();
		}	
	}
	
	@Override
    public StreamObserver<DownChunkResponse>
	executeToRecordSet(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				ExecutePlanRequest req = ExecutePlanRequest.parseFrom(header);
				Plan plan = Plan.fromProto(req.getPlan());
				
				RecordSet rset = m_runtime.executeToRecordSet(plan);
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
    public StreamObserver<DownChunkResponse>
	executeToStream(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender handler = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws Exception {
				GetStreamRequest req = GetStreamRequest.parseFrom(header);
				
				String id = req.getId();
				Plan plan = Plan.fromProto(req.getPlan());
				s_logger.debug("executeToStream: id={}", id);
				RecordSet rset = m_runtime.executeToStream(id, plan);
				return PBRecordProtos.toInputStream(rset);
			}
		};
		handler.start();
		
		return handler;
	}
	
	@Override
    public void getProcessRecordSchema(ExecuteProcessRequest req,
            							StreamObserver<RecordSchemaResponse> resp) {
		try {
			String procId = req.getProcessId();
			Map<String,String> params = PBValueProtos.fromProto(req.getParams());
			
			RecordSchema schema = m_runtime.getProcessOutputRecordSchema(procId, params);
			resp.onNext(RecordSchemaResponse.newBuilder()
												.setRecordSchema(schema.toProto())
												.build());
		}
		catch ( Exception e ) {
			resp.onNext(RecordSchemaResponse.newBuilder()
												.setError(PBUtils.toErrorProto(e))
												.build());
		}
		finally {
			resp.onCompleted();
		}
	}
	
	@Override
    public void executeProcess(ExecuteProcessRequest req, StreamObserver<VoidResponse> resp) {
		try {
			String procId = req.getProcessId();
			Map<String,String> params = PBValueProtos.fromProto(req.getParams());
			
			m_runtime.executeProcess(procId, params);
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
    public void ping(VoidProto req, StreamObserver<VoidResponse> resp) {
		try {
			resp.onNext(PBUtils.toVoidResponse());
		}
		catch ( Throwable e ) {
			resp.onNext(PBUtils.toVoidResponse(e));
		}
		finally {
			resp.onCompleted();
		}
	}
	
	private MarmotAnalysisResponse toMarmotAnalysisResponse(MarmotAnalysis analysis) {
		return MarmotAnalysisResponse.newBuilder()
										.setAnalysis(analysis.toProto())
										.build();
	}
	
	private MarmotAnalysisResponse toMarmotAnalysisResponse(Throwable e) {
		return MarmotAnalysisResponse.newBuilder()
										.setError(PBUtils.toErrorProto(e))
										.build();
	}
	
	private static ExecutionStateProto toExecutionStateProto(State state) {
		switch ( state ) {
			case RUNNING:
				return ExecutionStateProto.EXEC_RUNNING;
			case COMPLETED:
				return ExecutionStateProto.EXEC_COMPLETED;
			case CANCELLED:
				return ExecutionStateProto.EXEC_CANCELLED;
			case FAILED:
				return ExecutionStateProto.EXEC_FAILED;
			default:
				throw new AssertionError();
		}
	}
}
