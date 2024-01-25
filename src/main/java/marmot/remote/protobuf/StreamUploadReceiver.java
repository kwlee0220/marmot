package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CancellationException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.protobuf.PBUtils;
import marmot.protobuf.SuppliableInputStream;
import utils.LoggerSettable;
import utils.Throwables;
import utils.async.AbstractThreadedExecution;
import utils.async.Guard;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class StreamUploadReceiver implements StreamObserver<UpChunkRequest>, LoggerSettable {
	private final SuppliableInputStream m_stream;
	private volatile StreamObserver<UpChunkResponse> m_channel;
	private Logger m_logger = LoggerFactory.getLogger(StreamUploadReceiver.class);
	
	private volatile UploadedStreamProcessor m_streamConsumer = null;
	
	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private boolean m_eos = false;
	@GuardedBy("m_guard") private Object m_result = null;
	
	/**
	 * @param req	스트림 처리에 필요한 인자 정보들이 serialize된 바이트 배열.
	 * @param is	처리에 필요한 스트림.
	 * @return Upload된 스트림 처리 결과. 별도의 결과가 없는 경우는 {@code null}을 반환함.
	 */
	abstract protected ByteString consumeStream(ByteString req, InputStream is)
		throws Exception;
	
	public StreamUploadReceiver(StreamObserver<UpChunkResponse> channel) {
		m_stream = SuppliableInputStream.create(4);
		m_channel = channel;
	}

	@Override
	public Logger getLogger() {
		return m_logger;
	}

	@Override
	public void setLogger(Logger logger) {
		m_logger = logger;
	}

	@Override
	public void onNext(UpChunkRequest req) {
		switch ( req.getEitherCase() ) {
			case CHUNK:
				try {
					ByteString chunk = req.getChunk();
					getLogger().trace("received CHUNK[size={}]", chunk.size());
					m_stream.supply(chunk);
				}
				catch ( PBStreamClosedException e ) {
					m_streamConsumer.cancel(true);
				}
				catch ( InterruptedException e ) {
					m_stream.endOfSupply(new IOException("Chunk supplier is interrupted"));
					m_streamConsumer.cancel(true);
				}
				break;
			case SYNC:
				int sync = req.getSync();
				getLogger().debug("received SYNC[{}]", sync);
				m_channel.onNext(UpChunkResponse.newBuilder()
												.setSyncBack(sync)
												.build());
				break;
			case EOS:
				// sender측에서 스트림을 다 보내고나서 sync까지 마치고 보내는 메시지
				getLogger().debug("received END_OF_STREAM");
				
				// stream consumer에게 stream이 종료됨을 알려 완료될 수 있도록 한다.
				m_stream.endOfSupply();
				
				m_guard.run(() -> {
					m_eos = true;
					if ( m_result != null ) {
						getLogger().debug("disconnect");
						m_channel.onCompleted();
					}
				});
				break;
			case HEADER:
				// sender측에서 제일 먼저 보내는 메시지
				onHeaderReceived(req.getHeader());
				break;
			case ERROR:
				Exception cause = PBUtils.toException(req.getError());
				getLogger().info("received " + cause);
				m_stream.endOfSupply(cause);
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onCompleted() {
		// 클라이언트측에서 연결을 종료한 경우.
		getLogger().debug("disconnected");
		
		// 스트림이 종료되지 않는 경우에는 끊어진 사실을 알린다.
		// 만일 이미 종료된 상태라면 별다른 작업이 수행되지 않는다.
		m_stream.endOfSupply(new IOException("Uploader pipe is disconnected"));
		m_channel.onCompleted();
	}
	
	@Override
	public void onError(Throwable cause) {
		m_channel = null;
		
		if ( !m_stream.isClosed() ) {
			if ( cause instanceof StatusRuntimeException
				&& ((StatusRuntimeException)cause).getStatus().getCode() == Code.CANCELLED) {
				getLogger().info("client cancelled session");
				m_stream.endOfSupply(new IOException("Uploader pipe is disconnected"));
			}
			else {
				getLogger().warn("received SYSTEM_ERROR[cause=" + cause + "]");
				m_stream.endOfSupply(Throwables.unwrapThrowable(cause));
			}
		}
		else {
			// 클라이언트측에서 연결을 종료한 경우.
			getLogger().debug("disconnected");
		}
	}
	
	private void sendError(Throwable cause) {
		m_channel.onNext(UpChunkResponse.newBuilder()
											.setError(PBUtils.toErrorProto(cause))
											.build());
	}
	
	private void onHeaderReceived(ByteString header) {
		// 'handleStream()' 메소드가 실제 모든 작업이 종료되기 전에 반환될 수도 있기 때문에
		// 'StreamHandler' 비동기 연산이 완료(completed)되었다고 무조건 'onCompleted()' 메소드를
		// 호출하면 안된다.
		// 그러므로 비동기 연산이 종료되고, client쪽에서 stream이 모두 올라온 시점에서
		// onCompleted() 메소드를 호출한다.
		// 그러나 만일 비동기 연산이 실패이거나 취소인 경우는 바로 'onCompleted()'를 호출해도
		// 무방하다.
		//
		UploadedStreamProcessor handler = new UploadedStreamProcessor(header);
		handler.whenCompleted(ret -> {
			// Sender쪽에서는 무조건 result를 대기하기 때문에,
			// result가 없는 경우 (result == null)인 경우는 강제로 VoidProto를 전송시킨다.
			ByteString result = (ret != null) ? ret : PBUtils.VOID.toByteString();

			getLogger().debug("send RESULT: {}", result);
			m_channel.onNext(UpChunkResponse.newBuilder()
											.setResult(result)
											.build());
			m_guard.run(() -> {
				m_result = result;
				if ( m_eos ) {
					getLogger().debug("disconnect");
					m_channel.onCompleted();
				}
			});
		});
		handler.whenFailed(cause -> {
			if ( m_channel != null ) {
				getLogger().debug("send FAILED: {}", cause);
				sendError(cause);
				
				m_channel.onCompleted();
			}
		});
		handler.whenCancelled(() -> {
			if ( m_channel != null ) {
				getLogger().debug("send CANCELLED");
				sendError(new CancellationException());
				
				m_channel.onCompleted();
			}
		});
		handler.start();
		m_streamConsumer = handler;
	}
	
	private class UploadedStreamProcessor extends AbstractThreadedExecution<ByteString> {
		private final ByteString m_req;
		
		private UploadedStreamProcessor(ByteString req) {
			m_req = req;
		}
		
		@Override
		protected ByteString executeWork() throws Exception {
			return consumeStream(m_req, m_stream);
		}
	}
}