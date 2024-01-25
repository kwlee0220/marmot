package marmot.optor;

import com.google.common.base.Preconditions;

import marmot.Column;
import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.optor.reducer.TakeReducer;
import marmot.proto.optor.DistinctProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Distinct extends CompositeRecordSetFunction
						implements PBSerializable<DistinctProto> {
	private final MultiColumnKey m_distinctCols;
	private final FOption<Integer> m_workerCount;
	
	public Distinct(String keyCols, int workerCount) {
		Preconditions.checkArgument(workerCount > 0, "invalid workerCount: count=" + workerCount);
		
		m_distinctCols = toMultiColumnKey(keyCols);
		m_workerCount = FOption.of(workerCount);
	}
	
	public Distinct() {
		this(MultiColumnKey.EMPTY, FOption.empty());
	}
	
	public Distinct(String keyCols) {
		this(toMultiColumnKey(keyCols), FOption.empty());
	}
	
	private Distinct(MultiColumnKey keyCols, FOption<Integer> workerCount) {
		m_distinctCols = keyCols;
		m_workerCount = workerCount;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		return inputSchema;
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		// distinct 컬럼이 명시되지 않으면 전체 컬럼 값을 기분으로 grouping 한다.
		int ncols = m_inputSchema.getColumnCount();
		FOption<String> tagCols;
		if ( m_distinctCols.length() == ncols ) {
			tagCols = FOption.empty();
		}
		else {
			tagCols = FOption.of(m_distinctCols.complement(m_inputSchema)
												.toString());
		}
		
		TransformByGroup reduceByKey = TransformByGroup.builder()
												.keyColumns(m_distinctCols.toString())
												.tagColumns(tagCols)
												.workerCount(m_workerCount)
												.transform(TakeReducer.take(1))
												.build();
		
		// 'ReduceByGroupKey' 연산을 거치고나면 컬럼들의 순서가 바뀔 수 있기 때문에
		// 다시 원래 컬럼 순서로 배치는 project 연산을 추가한다.
		String colList = m_inputSchema.streamColumns()
									.map(Column::name)
									.join(',');
		Project project = new Project(colList);

		return RecordSetOperatorChain.from(m_marmot, m_inputSchema)
									.addAll(reduceByKey, project);
	}
	
	@Override
	public String toString() {
		return String.format("distinct[key=%s,nworkers=%s]",
							m_distinctCols, m_workerCount);
	}

	public static Distinct fromProto(DistinctProto proto) {
		switch ( proto.getOptionalWorkerCountCase() ) {
			case WORKER_COUNT:
				return new Distinct(proto.getKeyColumns(), proto.getWorkerCount());
			default:
				return new Distinct(proto.getKeyColumns());
		}
	}
	
	@Override
	public DistinctProto toProto() {
		DistinctProto.Builder builder = DistinctProto.newBuilder()
												.setKeyColumns(m_distinctCols.toString());
		m_workerCount.ifPresent(count -> builder.setWorkerCount(count));
		
		return builder.build();
	}
	
	private static MultiColumnKey toMultiColumnKey(String keyCols) {
		Preconditions.checkArgument(keyCols != null, "Distinct key columns is null");
		
		keyCols = keyCols.trim();
		return (keyCols == null || keyCols.length() == 0)
				? MultiColumnKey.EMPTY : MultiColumnKey.fromString(keyCols);
	}
}
