package marmot.optor;

import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.dataset.Catalogs;
import marmot.io.DataSetPartitionInfo;
import marmot.optor.support.AbstractRecordSetConsumer;
import marmot.proto.optor.UpdateDataSetInfoProto;
import marmot.support.PBSerializable;
import utils.Utilities;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class UpdateDataSetInfo extends AbstractRecordSetConsumer
							implements NonParallelizable, PBSerializable<UpdateDataSetInfoProto> {
	private final String m_dsId;
	private long m_count;
	
	public UpdateDataSetInfo(String dsId) {
		m_dsId = Catalogs.normalize(dsId);
		setLogger(LoggerFactory.getLogger(UpdateDataSetInfo.class));
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema);
	}

	@Override
	public void consume(RecordSet rset) {
		checkInitialized();
		Utilities.checkNotNullArgument(rset, "rset is null");
		
		DataSetPartitionInfo total = rset.fstream()
										.map(DataSetPartitionInfo::from)
										.reduce(DataSetPartitionInfo::plus);
		getLogger().info("done: {}", this);
		
		m_marmot.getCatalog().updateDataSetInfo(m_dsId, total);
	}
	
	@Override
	public String toString() {
		return String.format("%s: dataset='%s' count=%d", getClass().getSimpleName(),
							m_dsId, m_count);
	}

	public static UpdateDataSetInfo fromProto(UpdateDataSetInfoProto proto) {
		return new UpdateDataSetInfo(proto.getName());
	}

	@Override
	public UpdateDataSetInfoProto toProto() {
		return UpdateDataSetInfoProto.newBuilder()
						.setName(m_dsId)
						.build();
	}
}
