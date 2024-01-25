package marmot.optor.geo.index;

import static utils.UnitUtils.toByteSizeString;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.ReduceContext;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.GRecordSchema;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.dataset.GeometryColumnInfo;
import marmot.io.HdfsPath;
import marmot.io.MultiColumnKey;
import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.geo.index.SpatialIndexedCluster;
import marmot.io.geo.quadtree.QuadTree;
import marmot.mapreduce.MarmotMRContexts;
import marmot.mapreduce.ReduceContextKeyedRecordSetFactory;
import marmot.mapreduce.ReduceContextRecordSet;
import marmot.optor.geo.cluster.AttachQuadKeyRSet;
import marmot.optor.geo.cluster.Constants;
import marmot.optor.rset.InMemoryKeyedRecordSetFactory;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.optor.support.KeyedRecordSet;
import marmot.optor.support.KeyedRecordSetFactory;
import marmot.proto.optor.CreateIndexedClusterPackProto;
import marmot.rset.AbstractRecordSet;
import marmot.support.DefaultRecord;
import marmot.support.EnvelopeTaggedRecord;
import marmot.support.PBSerializable;
import marmot.support.ProgressReportable;
import marmot.type.DataType;
import marmot.type.MapTile;
import utils.StopWatch;
import utils.UnitUtils;
import utils.func.FOption;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateIndexedClusterPack extends AbstractRecordSetFunction
										implements PBSerializable<CreateIndexedClusterPackProto> {
	private static final Logger s_logger = LoggerFactory.getLogger(CreateIndexedClusterPack.class);

	private final Path m_dir;
	private final GeometryColumnInfo m_gcInfo;
	private final long m_blockSize;
	
	public CreateIndexedClusterPack(Path clusterDir, GeometryColumnInfo gcInfo, long blockSize) {
		m_dir = clusterDir;
		m_gcInfo = gcInfo;
		m_blockSize = blockSize;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, GlobalIndexEntry.SCHEMA);
	}

	@Override
	public Collecteds apply(RecordSet input) {
		checkInitialized();
		
		MultiColumnKey grpKey = MultiColumnKey.of(AttachQuadKeyRSet.COL_QUAD_KEY);
		
		KeyedRecordSetFactory groups;
		if ( input instanceof ReduceContextRecordSet ) {
			@SuppressWarnings("rawtypes")
			ReduceContext ctxt = ((ReduceContextRecordSet)input).getReduceContext();
			groups = new ReduceContextKeyedRecordSetFactory(ctxt, input.getRecordSchema(),
															grpKey, MultiColumnKey.EMPTY);
		}
		else {
			groups = new InMemoryKeyedRecordSetFactory(input, grpKey, false);
		}
		
		return new Collecteds(this, groups);
	}
	
	@Override
	public String toString() {
		return String.format("%s: dir=%s, block_size=%s]", getClass().getSimpleName(),
								m_dir, UnitUtils.toByteSizeString(m_blockSize));
	}
	
	public static CreateIndexedClusterPack fromProto(CreateIndexedClusterPackProto proto) {
		GeometryColumnInfo gcInfo = GeometryColumnInfo.fromProto(proto.getGeometryColumnInfo());
		return new CreateIndexedClusterPack(new Path(proto.getClusterDir()), gcInfo,
											proto.getBlockSize());
	}

	@Override
	public CreateIndexedClusterPackProto toProto() {
		return CreateIndexedClusterPackProto.newBuilder()
											.setClusterDir(m_dir.toString())
											.setGeometryColumnInfo(m_gcInfo.toProto())
											.setBlockSize(m_blockSize)
											.build();
	}

	private static class Collecteds extends AbstractRecordSet implements ProgressReportable {
		private final CreateIndexedClusterPack m_store;
		private final KeyedRecordSetFactory m_grsFact;
		private final Iterator<GlobalIndexEntry> m_idxIter;

		Collecteds(CreateIndexedClusterPack store, KeyedRecordSetFactory grsFact) {
			m_store = store;
			m_grsFact = grsFact;
			
			MarmotCore marmot = store.m_marmot;
			long maxBufferSize = store.m_blockSize * 3;
			try ( PackedClusterWriter writer
								= new PackedClusterWriter(marmot, store.m_dir, store.m_gcInfo,
															store.m_blockSize, maxBufferSize) ) {
				m_idxIter = writer.write(grsFact).iterator();
			}
			catch ( IOException e ) {
				throw new RecordSetException("fails to write clusters: cause=" + e);
			}
		}
		
		@Override protected void closeInGuard() { }

		@Override
		public RecordSchema getRecordSchema() {
			return GlobalIndexEntry.SCHEMA;
		}
	
		@Override
		public boolean next(Record record) {
			checkNotClosed();
			
			if ( !m_idxIter.hasNext() ) {
				return false;
			}
			
			GlobalIndexEntry idx = m_idxIter.next();
			idx.copyTo(record);
			
			return true;
		}
		
		@Override
		public void reportProgress(Logger logger, StopWatch elapsed) {
			m_grsFact.reportProgress(logger, elapsed);
			logger.info("report: {}", m_store.toString());
		}
	}
	
	private static class PackedClusterWriter implements Closeable {
		private final String m_packId;
		private final GeometryColumnInfo m_gcInfo;
		private final long m_blockSize;
		private final long m_maxBufferSize;
		private final FSDataOutputStream m_fos;
		
		PackedClusterWriter(MarmotCore marmot, Path clusterDir, GeometryColumnInfo gcInfo,
							long blockSize, long maxBufferSize) throws IOException {
			m_gcInfo = gcInfo;
			m_blockSize = blockSize;
			m_maxBufferSize = maxBufferSize;

			m_packId = String.format("%05d", MarmotMRContexts.getTaskOrdinal().getOrElse(0));
			HdfsPath path = HdfsPath.of(marmot.getHadoopConfiguration(),
										new Path(clusterDir, m_packId));
			m_fos = path.create(true, blockSize);
		}

		@Override
		public void close() throws IOException {
			m_fos.close();
		}
		
		private List<GlobalIndexEntry> write(KeyedRecordSetFactory grsFact)
			throws IOException {
			ClusterImageBuilder builder = new ClusterImageBuilder(m_gcInfo, m_blockSize);
			List<ClusterImage> images = Lists.newArrayList();
			List<GlobalIndexEntry> idxEntries = Lists.newArrayList();
			long totalSize = 0;
					
			int blockNo = -1;
			FOption<KeyedRecordSet> ogroup;
			while ( (ogroup = grsFact.nextKeyedRecordSet()).isPresent() ) {
				KeyedRecordSet clusterRSet = ogroup.get();
				String quadKey = (String)clusterRSet.getKey().getValueAt(0);
							
				ClusterImage image = builder.build(quadKey, clusterRSet);
				
				// image의 크기가 blockSize보다 크면 split시킨다.
				if ( image.length() > m_blockSize ) {
					List<ClusterImage> remains = image.split(m_gcInfo, m_blockSize);
					List<ClusterImage> splits = Lists.newArrayList();
					while ( !remains.isEmpty() ) {
						ClusterImage ci = remains.remove(0);
						if ( ci.length() < m_blockSize ) {
							splits.add(ci);
						}
						else {
							remains.addAll(ci.split(m_gcInfo, m_blockSize));
						}
					}
					
					for ( ClusterImage ci: splits ) {
						images.add(ci);
						totalSize += ci.m_length;
						if ( s_logger.isInfoEnabled() ) {
							s_logger.info("create cluster: {}, buffer_size={}",
											ci, toByteSizeString(totalSize));
						}
					}
				}
				else {
					images.add(image);
					totalSize += image.m_length;
					if ( s_logger.isInfoEnabled() ) {
						s_logger.info("create cluster: {}, buffer_size={}",
										image, toByteSizeString(totalSize));
					}
				}
				
				
				if ( totalSize >= m_maxBufferSize ) {
					Collections.sort(images);
					
					List<ClusterImage> block = selectPackedBlock(images, m_blockSize);
					idxEntries.addAll(writeBlock(++blockNo, block));
					
					totalSize -= block.stream().mapToLong(ci -> ci.m_length).sum();
				}
			}
			grsFact.closeQuietly();
			
			Collections.sort(images);
			while ( images.size() > 0 ) {
				List<ClusterImage> block = selectPackedBlock(images, m_blockSize);
				idxEntries.addAll(writeBlock(++blockNo, block));
			}
			
			return idxEntries;
		}
		
		private List<GlobalIndexEntry> writeBlock(int blockNo,
										List<ClusterImage> images) throws IOException {
			if ( s_logger.isDebugEnabled() ) {
				String ids = FStream.from(images).map(img -> img.toString()).join(", ");
				long total = images.stream().mapToLong(img -> img.length()).sum();
				s_logger.debug("write block: file={}:{}, clusters={}, total={}",
								m_packId, blockNo, ids, toByteSizeString(total));
			}
			
			List<GlobalIndexEntry> idxEntries = Lists.newArrayList();
			for ( ClusterImage image: images ) {
				image.m_idxEntry.packId(m_packId);
				image.m_idxEntry.blockNo(blockNo);
				image.m_idxEntry.start(m_fos.getPos());
				
				m_fos.write(image.m_bytes);
				s_logger.debug("\twrite cluster: {}", image);
				
				idxEntries.add(image.m_idxEntry);
			}
			long align = ((m_fos.getPos()+m_blockSize-1)/m_blockSize) * m_blockSize;
			long padSize = align - m_fos.getPos();
			if ( padSize > 0 ) {
				m_fos.write(new byte[(int)padSize]);
			}
			if ( m_fos.getPos() % m_blockSize != 0 ) {
				throw new AssertionError("incorrect block alignment");
			}
			
			return idxEntries;
		}
		
		private static List<ClusterImage> selectPackedBlock(List<ClusterImage> images,
															long blockSize)  {
			List<ClusterImage> selecteds = Lists.newArrayList();
			long remains = blockSize;
			
			while ( images.size() > 0 ) {
				ClusterImage selected = removeLargest(images, remains);
				if ( selected == null ) {
					break;
				}
				remains -= selected.m_bytes.length;
				selecteds.add(selected);
			}
			
			return selecteds;
		}
		
		private static ClusterImage removeLargest(List<ClusterImage> images, long remains) {
			for ( int i  =0; i < images.size(); ++i ) {
				ClusterImage img = images.get(i);
				
				if ( img.m_bytes.length <= remains ) {
					return images.remove(i);
				}
			}
			
			return null;
		}
	}
	
	private static class ClusterImageBuilder {
		private final GeometryColumnInfo m_gcInfo;
		private final long m_blockSize;
		
		ClusterImageBuilder(GeometryColumnInfo gcInfo, long blockSize) {
			m_gcInfo = gcInfo;
			m_blockSize = blockSize;
		}
		
		ClusterImage build(String quadKey, RecordSet clusterRSet) {
			RecordSchema dataSchema = clusterRSet.getRecordSchema()
												.complement(Constants.COL_MBR, Constants.COL_QUAD_KEY);
			GRecordSchema gschema = new GRecordSchema(m_gcInfo, dataSchema);
			
			FStream<EnvelopeTaggedRecord> recs = fromTaggedRecordSet(clusterRSet);
			SpatialIndexedCluster cluster
								= SpatialIndexedCluster.build(quadKey, gschema, recs);
			byte[] bytes = cluster.toBytes((int)m_blockSize);
			
			GlobalIndexEntry idx = new GlobalIndexEntry("", 0, quadKey, cluster.getDataBounds(),
														(int)cluster.getRecordCount(),
														(int)cluster.getOwnedRecordCount(),
														0, bytes.length);
			return new ClusterImage(idx, bytes);
		}
		
		FStream<EnvelopeTaggedRecord> fromTaggedRecordSet(RecordSet rset) {
			int mbrColIdx = rset.getRecordSchema().getColumn(Constants.COL_MBR).ordinal();
			List<String> tagCols = Lists.newArrayList(Constants.COL_MBR, Constants.COL_QUAD_KEY);
			RecordSchema dataSchema = rset.getRecordSchema().complement(tagCols);
			int[] dataColIdxes = dataSchema.streamColumns().mapToInt(c -> c.ordinal()).toArray();
			
			return rset.fstream()
						.map(rec -> {
							Envelope mbr = (Envelope)rec.get(mbrColIdx);
							
							Object[] values = new Object[dataColIdxes.length];
							for ( int i =0; i < dataColIdxes.length; ++i ) {
								values[i] = rec.get(dataColIdxes[i]);
							}
							
							Record record = DefaultRecord.of(dataSchema);
							record.setAll(values);
							return new EnvelopeTaggedRecord(mbr, record);
						});
		}
	}
	
	public static class ClusterImage implements Comparable<ClusterImage> {
		private final GlobalIndexEntry m_idxEntry;
		private final byte[] m_bytes;
		private final int m_length;
		
		public ClusterImage(GlobalIndexEntry idx, byte[] bytes) {
			m_idxEntry = idx;
			m_bytes = bytes;
			m_length = bytes.length;
		}
		
		String quadKey() {
			return m_idxEntry.quadKey();
		}
		
		long length() {
			return m_idxEntry.length();
		}

		@Override
		public int compareTo(ClusterImage ci) {
			return ci.m_length - m_length;
		}
		
		@Override
		public String toString() {
			return String.format("%s(%s)", quadKey(), UnitUtils.toByteSizeString(m_length));
		}
		
		static class Split {
			String m_quadKey;
			Envelope m_tileBounds;
			List<Record> m_records;
			
			Split(String quadKey) {
				m_quadKey = quadKey;
				m_tileBounds =  MapTile.fromQuadKey(m_quadKey).getBounds();
				m_records = Lists.newArrayList();
			}
			
			boolean intersects(Envelope bounds) {
				return m_tileBounds.intersects(bounds);
			}
			
			void add(Record record) {
				m_records.add(record);
			}
			
			ClusterImage buildClusterImage(GeometryColumnInfo gcInfo, long blockSize) {
				ClusterImageBuilder builder = new ClusterImageBuilder(gcInfo, blockSize);
				return builder.build(m_quadKey, RecordSet.from(m_records));
			}
		}
		
		private List<ClusterImage> split(GeometryColumnInfo gcInfo, long blockSize) {
			if ( s_logger.isInfoEnabled() ) {
				s_logger.info("splitting " + this);
			}
			
			List<Split> splits = Lists.newArrayList();
			for ( int i =0; i < QuadTree.QUAD; ++i ) {
				String subKey = m_idxEntry.quadKey() + i;
				splits.add(new Split(subKey));
			}
			
			SpatialIndexedCluster cluster = SpatialIndexedCluster.fromBytes(m_bytes, 0, m_bytes.length);
//			HdfsSpatialIndexedCluster cluster = MarmotSerializers.fromBytes(m_bytes, HdfsSpatialIndexedCluster::deserialize);
			RecordSchema expandedSchema = cluster.getRecordSchema().toBuilder()
												.addColumn(AttachQuadKeyRSet.COL_MBR, DataType.ENVELOPE)
												.build();

			cluster.read(false)
					.forEach(etr -> {
						Envelope mbr = etr.getEnvelope();

						Record expanded = DefaultRecord.of(expandedSchema);
						expanded.set(etr.getRecord());
						expanded.set(AttachQuadKeyRSet.COL_MBR, mbr);
						
						for ( Split split: splits ) {
							if ( split.intersects(mbr) ) {
								split.add(expanded);
							}
						}
					});
			
			List<ClusterImage> splittedImages = Lists.newArrayList();
			for ( Split split: splits ) {
				if ( split.m_records.size() > 0 ) {
					splittedImages.add(split.buildClusterImage(gcInfo, blockSize));
				}
			}
			return splittedImages;
		}
	}
}
