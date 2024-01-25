package marmot.remote.protobuf;

import io.grpc.stub.StreamObserver;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpRequestDownResponse;
import marmot.protobuf.PBUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StreamUpnDownloadServer implements StreamObserver<UpRequestDownResponse> {
	private final StreamObserver<UpChunkRequest> m_upChannel;
	private final StreamObserver<DownChunkResponse> m_downChannel;
	
	public StreamUpnDownloadServer(StreamObserver<UpChunkRequest> upChannel,
							StreamObserver<DownChunkResponse> downChannel) {
		m_upChannel = upChannel;
		m_downChannel = downChannel;
	}

	@Override
	public void onNext(UpRequestDownResponse msg) {
		switch ( msg.getEitherCase() ) {
			case UP_REQ:
				m_upChannel.onNext(msg.getUpReq());
				break;
			case DOWN_RESP:
				m_downChannel.onNext(msg.getDownResp());
				break;
			case UP_CLOSED:
				m_upChannel.onCompleted();
				break;
			case DOWN_CLOSED:
				m_downChannel.onCompleted();
				break;
			case UP_ERROR:
				m_upChannel.onError(PBUtils.toException(msg.getUpError()));
				break;
			case DOWN_ERROR:
				m_downChannel.onError(PBUtils.toException(msg.getDownError()));
			default:
				throw new AssertionError();
		}
	}

	@Override
	public void onError(Throwable cause) {
		m_upChannel.onError(cause);
		m_downChannel.onError(cause);
	}

	@Override
	public void onCompleted() {
		m_upChannel.onCompleted();
		m_downChannel.onCompleted();
	}

}
