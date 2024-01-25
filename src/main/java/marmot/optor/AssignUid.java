package marmot.optor;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.mapreduce.MarmotMRContext;
import marmot.mapreduce.MarmotMRContextAware;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.AssignUidProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AssignUid extends RecordLevelTransform
						implements MarmotMRContextAware, PBSerializable<AssignUidProto> {
	private static final int MAX_RECS_PER_SPLIT = 10_000_000;
	
	private final String m_uidColumn;
	private final int m_maxRecordPerSplit;
	
	private int m_uidColIdx;
	private long m_startId = -1;	// just for 'toString()'
	private long m_idGen = 0;
	
	public AssignUid(String colName) {
		this(colName, MAX_RECS_PER_SPLIT);
	}
	
	public AssignUid(String colName, int nrecs) {
		Utilities.checkNotNullArgument(colName, "id column");
		Preconditions.checkArgument(nrecs > 0, "invalid maximum records per split");

		m_uidColumn = colName;
		m_maxRecordPerSplit = nrecs;
	}
	
	public String getUidColumn() {
		return m_uidColumn;
	}
	
	public int getMaximumRecordsPerSplit() {
		return m_maxRecordPerSplit;
	}

	@Override
	public void setMarmotMRContext(MarmotMRContext context) {
		int splitNo = context.getTaskOrdinal();
		m_idGen = m_startId = (long)m_maxRecordPerSplit * splitNo;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema outSchema = inputSchema.toBuilder()
											.addOrReplaceColumn(m_uidColumn, DataType.LONG)
											.build();
		m_uidColIdx = outSchema.getColumn(m_uidColumn).ordinal();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		checkInitialized();
		
		output.set(input);
		output.set(m_uidColIdx, m_idGen++);
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("assign_uid[%s:start=%d,next=%d]", m_uidColumn, m_startId, m_idGen);
	}

	public static AssignUid fromProto(AssignUidProto proto) {
		return new AssignUid(proto.getUidColumn());
	}

	@Override
	public AssignUidProto toProto() {
		return AssignUidProto.newBuilder()
							.setUidColumn(m_uidColumn)
							.build();
	}
}