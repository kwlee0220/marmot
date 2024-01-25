package marmot.remote.protobuf;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;
import marmot.MarmotCore;
import marmot.RecordSet;
import marmot.dataset.MarmotFileServer;
import marmot.proto.StringProto;
import marmot.proto.service.CopyToHdfsFileRequest;
import marmot.proto.service.DownChunkRequest;
import marmot.proto.service.DownChunkResponse;
import marmot.proto.service.FileServiceGrpc.FileServiceImplBase;
import marmot.proto.service.UpChunkRequest;
import marmot.proto.service.UpChunkResponse;
import marmot.proto.service.VoidResponse;
import marmot.protobuf.PBRecordProtos;
import marmot.protobuf.PBUtils;
import utils.func.FOption;
import utils.io.Lz4Compressions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBFileServiceServant extends FileServiceImplBase {
	private static final Logger s_logger = LoggerFactory.getLogger(PBFileServiceServant.class);
	
	private final MarmotCore m_marmot;
	private final MarmotFileServer m_fileServer;

	public PBFileServiceServant(MarmotCore marmot) {
		m_marmot = marmot;
		m_fileServer = marmot.getFileServer();
	}

	@Override
    public StreamObserver<DownChunkResponse> readMarmotFile(StreamObserver<DownChunkRequest> channel) {
		StreamDownloadSender sender = new StreamDownloadSender(channel) {
			@Override
			protected InputStream getStream(ByteString header) throws IOException {
				Path path = new Path(StringProto.parseFrom(header).getValue());
				
				RecordSet rset = m_fileServer.readMarmotFile(path);
				s_logger.debug("readMarmotFile: path={}", path);
				return PBRecordProtos.toInputStream(rset);
			}
		};
		sender.start();
		
		return sender;
	}
	
	@Override
    public void deleteHdfsFile(StringProto req, StreamObserver<VoidResponse> resp) {
		PBUtils.replyVoid(() -> m_fileServer.deleteFile(new Path(req.getValue())), resp);
	}
	
	@Override
    public StreamObserver<UpChunkRequest>
	copyToHdfsFile(StreamObserver<UpChunkResponse> channel) {
		return new StreamUploadReceiver(channel) {
			@Override
			protected ByteString consumeStream(ByteString header, InputStream is)
				throws Exception {
				CopyToHdfsFileRequest req = CopyToHdfsFileRequest.parseFrom(header);
				
				String path = PBUtils.fromProto(req.getPath());
				FOption<Long> blockSize = FOption.empty();
				switch ( req.getOptionalBlockSizeCase() ) {
					case BLOCK_SIZE:
						blockSize = FOption.of(req.getBlockSize());
						break;
					case OPTIONALBLOCKSIZE_NOT_SET:
						break;
					default:
						throw new AssertionError();
				}
				
				FOption<String> codecName = FOption.empty();
				switch ( req.getOptionalCompressionCodecNameCase() ) {
					case COMPRESSION_CODEC_NAME:
						codecName = FOption.of(req.getCompressionCodecName());
						break;
					case OPTIONALCOMPRESSIONCODECNAME_NOT_SET:
						break;
					default:
						throw new AssertionError();
				}
				if ( req.getUseCompression() ) {
					is = Lz4Compressions.decompress(is);
				}

				try ( InputStream cis = is ) {
					long nbytes = m_fileServer.copyToHdfsFile(path, cis, blockSize, codecName);
					return PBUtils.toLongResponse(nbytes).toByteString();
				}
			}
		};
	}
}
