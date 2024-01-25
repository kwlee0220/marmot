package marmot.optor.join;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.io.MultiColumnKey;
import marmot.io.RecordKey;
import marmot.proto.optor.LoadMarmotFileProto;
import marmot.proto.optor.OperatorProto;
import marmot.support.PlanUtils;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MemoryLookupTable implements LookupTable {
	private final MultiColumnKey m_keyCols;
	private final Plan m_loadPlan;

	private MarmotCore m_marmot;
	private RecordSchema m_schema;
	private Map<RecordKey,List<Record>> m_table;
	
	public static MemoryLookupTable from(MultiColumnKey keyCols, Plan loadPlan) {
		return new MemoryLookupTable(keyCols, loadPlan);
	}
	
	public static MemoryLookupTable from(MultiColumnKey keyCols, String filePath) {
		return new MemoryLookupTable(keyCols, Plan.builder().loadMarmotFile(filePath).build());
	}
	
	public static MemoryLookupTable from(MultiColumnKey keyCols, Path filePath, Plan loadPlan) {
		LoadMarmotFileProto load = LoadMarmotFileProto.newBuilder()
													.addAllPaths(Arrays.asList(filePath.toString()))
													.build();
		OperatorProto proto = OperatorProto.newBuilder()
											.setLoadMarmotfile(load)
											.build();
		Plan extended =	PlanUtils.prependOperator(loadPlan, proto);
		return new MemoryLookupTable(keyCols, extended);
	}
	
	private MemoryLookupTable(MultiColumnKey keyCols, Plan loadPlan) {
		Preconditions.checkArgument(keyCols != null, "LookupTable key column is null");
		Preconditions.checkArgument(loadPlan != null, "MarmotFile load plan for LookupTable is null");
		
		m_keyCols = keyCols;
		m_loadPlan = loadPlan;
	}

	@Override
	public MultiColumnKey getKeyColumns() {
		return m_keyCols;
	}

	@Override
	public void open(MarmotCore marmot) {
		m_marmot = marmot;

		m_schema = marmot.getOutputRecordSchema(m_loadPlan);
		List<String> badKeys = FStream.from(m_keyCols.getColumnNames())
										.filter(n -> !m_schema.existsColumn(n))
										.toList();
		if ( badKeys.size() > 0 ) {
			throw new IllegalArgumentException("invalid look-up keys=" + badKeys);
		}
		
		loadFile();
	}

	@Override
	public void close() {
		m_marmot = null;
	}

	@Override
	public RecordSchema getRecordSchema() {
		Preconditions.checkState(m_marmot != null, "Marmot has not been set");

		return m_schema;
	}

	@Override
	public Set<RecordKey> getDistinctKeys() {
		Preconditions.checkState(m_marmot != null, "Marmot has not been set");
		Preconditions.checkState(m_keyCols != null, "key has not been set");
		
		return m_table.keySet();
	}

	@Override
	public RecordSet lookup(Object[] keyValues) {
		List<Record> bucket = m_table.getOrDefault(RecordKey.from(keyValues), Collections.emptyList());
		return RecordSet.from(m_schema, bucket);
	}
	
	private void loadFile() {
		m_table = Maps.newHashMap();

		RecordSet result = m_marmot.executeLocally(m_loadPlan);
		result.forEach(record -> {
			RecordKey key = RecordKey.from(m_keyCols, record);
			
			m_table.computeIfAbsent(key, k -> Lists.newArrayList())
					.add(record.duplicate());
		});
	}
}
