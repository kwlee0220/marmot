package marmot.optor;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import marmot.MarmotCore;
import marmot.RecordSchema;
import marmot.mapreduce.StageWorkspaceAware;
import marmot.plan.LoadOptions;
import marmot.proto.optor.StoreAndReloadProto;
import marmot.support.PBSerializable;
import marmot.support.RecordSetOperatorChain;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class StoreAndReload extends CompositeRecordSetFunction
							implements StageWorkspaceAware, PBSerializable<StoreAndReloadProto> {
	private final LoadOptions m_options;
	
	private volatile Path m_workspace = null;
	
	private StoreAndReload(LoadOptions opts) {
		m_options = opts;
	}

	@Override
	public boolean isMapReduceRequired() {
		return true;
	}

	@Override
	public void setStageWorkspace(Path workspaceDir) {
		m_workspace = workspaceDir;
	}

	@Override
	protected RecordSchema _initialize(MarmotCore marmot, RecordSchema inputSchema) {
		return inputSchema;
	}

	@Override
	protected RecordSetOperatorChain createComponents() {
		Preconditions.checkState(m_workspace != null, "stage workspace is not set: optor=" + this);

		Path outputPath = new Path(m_workspace, "output");
		
		// 연산 결과를 임시 파일에 저장하는 연산자를 생성하여 추가 
		StoreAsHeapfile store = new StoreAsHeapfile(outputPath);
		
		// 다음 MR 단계에서 생성된 임시파일을 읽는 연산 추가
		RecordSetLoader load = new LoadMarmotFile(outputPath.toString(), m_options);
		
		return RecordSetOperatorChain.from(m_marmot, m_inputSchema)
									.addAll(store, load);
	}
	
	@Override
	public String toString() {
		return String.format("store_and_reload[split_per_block=%d]",
							m_options.splitCount().getOrElse(1));
	}
	
	public static StoreAndReload fromProto(StoreAndReloadProto proto) {
		LoadOptions opts = LoadOptions.fromProto(proto.getOptions());
		return new StoreAndReload(opts);
	}

	@Override
	public StoreAndReloadProto toProto() {
		return StoreAndReloadProto.newBuilder()
									.setOptions(m_options.toProto())
									.build();
	}
}
