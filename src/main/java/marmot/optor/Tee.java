package marmot.optor;

import javax.annotation.Nullable;

import org.apache.hadoop.fs.Path;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.DataSetPartitionInfo;
import marmot.io.HdfsPath;
import marmot.io.MarmotFileWriteOptions;
import marmot.io.MarmotSequenceFile;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.rset.SingleInputRecordSet;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.TeeProto;
import marmot.proto.optor.TeeProto.GeometryInfoProto;
import marmot.rset.PeekableRecordSet;
import marmot.support.PBSerializable;
import utils.Utilities;
import utils.func.FOption;
import utils.io.IOUtils;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Tee extends AbstractRecordSetFunction implements PBSerializable<TeeProto> {
	private final Path m_path;
	private final MarmotFileWriteOptions m_options;
	private @Nullable Path m_infoPath;
	private @Nullable GeometryColumnInfo m_gcInfo;
	
	public Tee(Path path, MarmotFileWriteOptions opts) {
		Utilities.checkNotNullArgument(path, "path is null");
		Utilities.checkNotNullArgument(opts, "MarmotFileWriteOptions is null");
		
		m_path = path;
		m_options = opts;
		
		setLogger(LoggerFactory.getLogger(Tee.class));
	}
	
	public Tee setGeometryInfo(Path infoPath, GeometryColumnInfo gcInfo) {
		m_infoPath = infoPath;
		m_gcInfo = gcInfo;
		return this;
	}
	
	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		PeekableRecordSet peekable = RecordSets.toPeekable(input);
		return new StoreAndPassed(this, peekable);
	}
	
	private static class StoreAndPassed extends SingleInputRecordSet<Tee> {
		private final MarmotSequenceFile.Writer m_writer;
		
		private int m_geomColIdx = -1;
		private HdfsPath m_infoPath;
		private Envelope m_mbr = new Envelope();
		private long m_count = 0;
		
		protected StoreAndPassed(Tee func, RecordSet input) {
			super(func, input);
			
			RecordSchema schema = input.getRecordSchema();
			
			PeekableRecordSet peekable = (PeekableRecordSet)m_input;
			if ( peekable.peek().isPresent() ) {
				MarmotCore marmot = (MarmotCore)func.getMarmotCore();

				HdfsPath path = HdfsPath.of(marmot.getHadoopFileSystem(), func.m_path);
				if ( m_optor.m_options.force() ) {
					path.delete();
				}
				
				if ( m_optor.m_infoPath != null ) {
					m_geomColIdx = schema.getColumn(m_optor.m_gcInfo.name()).ordinal();
					
					m_infoPath = HdfsPath.of(marmot.getHadoopFileSystem(), m_optor.m_infoPath);
					FOption<Integer> ordinal = MarmotMRContexts.getTaskOrdinal();
					if ( ordinal.isPresent() ) {
						String taskId = "" + ordinal.getUnchecked();
						
						path = path.child(taskId);
						m_infoPath = m_infoPath.child(taskId);
					}
				}
				
				m_writer = MarmotSequenceFile.create(path, getInputSchema(), null, m_optor.m_options);
			}
			else {
				m_writer = null;
			}
		}

		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_writer);
			
			super.closeInGuard();
		}

		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( m_input.next(record) ) {
				m_writer.write(record);
				if ( m_geomColIdx >= 0 ) {
					Envelope mbr = record.getGeometry(m_geomColIdx).getEnvelopeInternal();
					m_mbr.expandToInclude(mbr);
				}
				++m_count;
				
				return true;
			}

			long size = m_writer.getLength();
			IOUtils.closeQuietly(m_writer);
			
			if ( m_infoPath != null ) {
				DataSetPartitionInfo info = new DataSetPartitionInfo(m_mbr, m_count, size);
				RecordSet rset = RecordSet.of(info.toRecord());
				
				MarmotSequenceFile.store(m_infoPath, rset, null, m_optor.m_options).call();
			}
			
			return false;
		}
	}
	
	@Override
	public String toString() {
		return String.format("tee: path=%s opts=%s", m_path, m_options);
	}

	public static Tee fromProto(TeeProto proto) {
		MarmotFileWriteOptions opts = MarmotFileWriteOptions.fromProto(proto.getWriteOptions());
		
		Tee tee = new Tee(new Path(proto.getPath()), opts);
		switch ( proto.getOptionalGeomInfoCase()) {
			case GEOM_INFO:
				GeometryInfoProto giProto = proto.getGeomInfo();
				tee.setGeometryInfo(new Path(giProto.getInfoPath()),
									GeometryColumnInfo.fromProto(giProto.getGcInfo()));
				break;
			case OPTIONALGEOMINFO_NOT_SET:
				break;
			default:
				throw new AssertionError();
		}
		
		return tee;
	}

	@Override
	public TeeProto toProto() {
		GeometryInfoProto geomInfo = null;
		if ( m_infoPath != null ) {
			geomInfo = GeometryInfoProto.newBuilder()
										.setGcInfo(m_gcInfo.toProto())
										.setInfoPath(m_infoPath.toString())
										.build();
		}
		
		TeeProto.Builder builder = TeeProto.newBuilder()
											.setPath(m_path.toString())
											.setWriteOptions(m_options.toProto());
		if ( geomInfo != null ) {
			builder = builder.setGeomInfo(geomInfo);
		}
		
		return builder.build();
	}
}
