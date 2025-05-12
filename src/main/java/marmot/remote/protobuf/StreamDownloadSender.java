package marmot.remote.protobuf;

import java.io.InputStream;
import java.util.Date;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import utils.LoggerSettable;
import utils.Throwables;
import utils.Utilities;
import utils.async.AbstractThreadedExecution;
import utils.async.Guard;
import utils.func.Try;
import utils.io.IOUtils;
import utils.io.LimitedInputStream;

import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamDownloadSender extends AbstractThreadedExecution<Void>
							implements StreamObserver<DownChunkResponse>, LoggerSettable {
	private static final Logger s_logger = LoggerFactory.getLogger(StreamDownloadSender.class);
	
	private static final int DEFAULT_CHUNK_SIZE = 64 * 1024;
	private static final int SYNC_INTERVAL = 4;
	private static final long MAX_GET_STREAM_TIMEOUT = 5;	// 5 minutes
	private static final long MAX_SYNC_TIMEOUT = 30;		// 10s
	
	private final StreamObserver<DownChunkRequest> m_channel;
	private int m_chunkSize = DEFAULT_CHUNK_SIZE;

	private final Guard m_guard = Guard.create();
	@GuardedBy("m_guard") private InputStream m_stream = null;
	@GuardedBy("m_guard") private int m_syncBack = 0;
	@GuardedBy("m_guard") private State m_state = State.NOT_STARTED;
	
	private static enum State {
		NOT_STARTED,
		WAIT_STREAM,
		DOWNLOADING,
		CANCELLING,
		CANCELLED,
		TIMED_OUT,
	}
	
	protected InputStream getStream(ByteString header) throws Exception { 
		throw new AssertionError("this method should be replaced at subclass");
	}
	
	/**
	 * 주어진 InputStream을 download시킨다.
	 * Download 대상이되는 스트림은 다음과 같은 두가지 경우가 있다.
	 * <ul>
	 * 	<li> 클라이언트에서 'HEADER' 메시지가 도착한 경우, {@link #getStream(ByteString)} 메소드를
	 * 		호출하여 결과로 받은 스트림을 획득한다.
	 * 	<li> 호출자가 명시적으로 {@link #setInputStream(InputStream)}를 호출하여 설정된 경우.
	 * </ul>
	 * 
	 * @param channel	Download시킬 데이터를 전송할 출력 채널.
	 */
	public StreamDownloadSender(StreamObserver<DownChunkRequest> channel) {
		Utilities.checkNotNullArgument(channel, "Download message channel");
		
		m_channel = channel;
		setLogger(s_logger);
	}
	
	// download receiver의 요청에 의해 stream이 생성된 것이 아니라,
	// 로컬에서 stream 을 생성하여 설정하는 경우.
	//
	public void setInputStream(InputStream stream) {
		Utilities.checkNotNullArgument(stream, "Download stream");
		
		m_guard.run(() -> {
			if ( m_state != State.NOT_STARTED ) {
				throw new IllegalStateException("state=" + m_state + ", expected=" + State.NOT_STARTED);
			}
			
			m_stream = stream;
			m_state = State.DOWNLOADING;
		});
	}
	
	public StreamDownloadSender chunkSize(int size) {
		Preconditions.checkArgument(size > 0, "chunkSize > 0");
		
		m_chunkSize = size;
		return this;
	}

	@Override
	protected Void executeWork() throws CancellationException, Exception {
		// download시킬 스트림이 준비될 때까지 대기한다.
		// 클라이언트 측에서 헤더 정보가 오지 않을 수도 있기 때문에 timeout를 설정한다.
		waitUntilDownloadIsReady();
		
		try {	
			int chunkCount = 0;
			
			// chunk를 보내는 과정 중에 자체적으로 또는 상대방쪽에서
			// 오류가 발생되거나, 취소시킬 수 있으니 확인한다.
			while ( isRunning() ) {
				LimitedInputStream chunkedStream = new LimitedInputStream(m_stream, m_chunkSize);
				ByteString chunk = ByteString.readFrom(chunkedStream);
				if ( chunk.isEmpty() ) {
					// 최종 확인차 마지막 chunk에 대한 sync를 보내고, sync-back을 대기한다.
					try {
						sync(chunkCount, chunkCount);
					}
					catch ( InterruptedException e ) {
						break;
					}
					
					m_channel.onNext(DownChunkRequest.newBuilder().setEos(PBUtils.VOID).build());
					getLogger().debug("send END_OF_STREAM");
					
					break;
				}

				sendChunk(chunk);
				++chunkCount;
				getLogger().trace("send CHUNK[idx={}, size={}]", chunkCount, chunk.size());

				if ( (chunkCount % SYNC_INTERVAL) == 0 ) {
					sync(chunkCount, chunkCount-SYNC_INTERVAL);
				}
			}
			
			return null;
		}
		catch ( Throwable e ) {
			if ( isRunning() ) {
				sendError(Throwables.unwrapThrowable(e));
			}
			
			return null;
		}
		finally {
			getLogger().debug("close CONNECTION");
			
			Try.run(() -> m_channel.onCompleted());
			IOUtils.closeQuietly(m_stream);
		}
	}

	@Override
	public void onNext(DownChunkResponse req) {
		switch ( req.getEitherCase() ) {
			case SYNC_BACK:
				getLogger().debug("received SYNC_BACK[{}]", req.getSyncBack());
				m_guard.run(() -> m_syncBack = req.getSyncBack());
				break;
			case HEADER:
				ByteString header = req.getHeader();
				getLogger().debug("received HEADER[size={}]", header.size());
				
				m_guard.runChecked(() -> {
					if ( m_state != State.NOT_STARTED ) {
						Throwable cause = new IllegalArgumentException("state=" + m_state
																	+ ", expected=" + State.NOT_STARTED);
						sendError(cause);
						m_channel.onCompleted();
						
						notifyFailed(cause);
					}
					
					m_state = State.WAIT_STREAM;
				});

				try {
					InputStream stream = getStream(header);
					
					m_guard.runChecked(() -> {
						if ( m_state != State.WAIT_STREAM ) {
							throw new IllegalStateException("state=" + m_state + ", expected=" + State.WAIT_STREAM);
						}
						
						m_stream = stream;
						m_state = State.DOWNLOADING;
					});
				}
				catch ( Exception e ) {
					Throwable cause = Throwables.unwrapThrowable(e);
					getLogger().warn("fails to create Stream, cause={}", cause.toString());

					// 클라이언트측에서 올라온 오류와, 스트림을 전송하다가 자체적으로 발생된
					// 예외에 대한 처리 방식을 달리하기 위해, 자체적으로 발생된 예외는
					// 비동기 수행을 실패시키기 전에 명시적으로 예외 메시지를 클라이언트에 보내는
					// 작업을 수행한다.
					sendError(cause);
					m_channel.onCompleted();
					
					notifyFailed(cause);
				}
				break;
			case CANCEL:
				onClientCancelled();
				break;
			case ERROR:
				Exception cause = PBUtils.toException(req.getError());
				if ( cause instanceof PBStreamClosedException ) {
					getLogger().debug("receiver has closed the stream");
					notifyCancelled();
				}
				else {
					getLogger().info("received PEER_ERROR[cause={}]", cause.toString());
					notifyFailed(cause);
				}
				break;
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		if ( PBUtils.isUnavailableException(cause) ) {
			getLogger().info("Receiver has closed the connection");
			cancel(true);
		}
		else {
			getLogger().warn("received SYSTEM_ERROR[cause=" + cause + "]");
			notifyFailed(cause);
		}
	}

	@Override
	public void onCompleted() {
		cancel(true);
	}
	
	private int sync(int sync, int expectedSyncBack) throws InterruptedException, TimeoutException {
		m_channel.onNext(DownChunkRequest.newBuilder().setSync(sync).build());
		getLogger().debug("sent SYNC[{}] & wait for SYNC_BACK[{}]",
							sync, expectedSyncBack);
		
		Date due = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(MAX_SYNC_TIMEOUT));
		
		m_guard.lock();
		try {
			while ( true ) {
				if ( !isRunning() ) {
					getLogger().debug("waitForSync is interrupted: state={}", getState());
					throw new InterruptedException();
				}
				if ( m_syncBack >= expectedSyncBack ) {
					return m_syncBack;
				}
				
				if ( !m_guard.awaitSignal(due) ) {
					throw new TimeoutException("sync timeout");
				}
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void sendChunk(ByteString chunk) {
		m_channel.onNext(DownChunkRequest.newBuilder().setChunk(chunk).build());
	}
	
	private void sendError(Throwable cause) {
		m_channel.onNext(DownChunkRequest.newBuilder()
											.setError(PBUtils.toErrorProto(cause))
											.build());
	}
	
	private void waitUntilDownloadIsReady()
		throws InterruptedException, TimeoutException, Exception {
		Date due = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(MAX_GET_STREAM_TIMEOUT));
		m_guard.lock();
		try {
			while ( m_state != State.DOWNLOADING ) {
				if ( m_state == State.NOT_STARTED || m_state == State.WAIT_STREAM ) {
					if ( !m_guard.awaitSignal(due) ) {
						String msg = String.format("Stream is not available within %d minutes",
													MAX_GET_STREAM_TIMEOUT);
						Throwable cause = new TimeoutException(msg);
						sendError(cause);
						m_channel.onCompleted();
						
						throw new TimeoutException(msg);
					}
				}
				else {
					Exception cause = new IllegalArgumentException("state=" + m_state
														+ ", expected=" + State.DOWNLOADING);
					sendError(cause);
					m_channel.onCompleted();
					
					throw cause;
				}
			}
		}
		finally {
			m_guard.unlock();
		}
	}
	
	private void onClientCancelled() {
		getLogger().debug("received CANCEL");
		
		m_guard.run(() -> {
			if ( m_state == State.DOWNLOADING ) {
				m_channel.onNext(DownChunkRequest.newBuilder().setEos(PBUtils.VOID).build());
				getLogger().debug("send END_OF_STREAM (Cancelled)");
				
				m_state = State.CANCELLING;
			}
		});
		cancel(true);
		
		m_guard.run(() -> m_state = State.CANCELLED);
	}
}
