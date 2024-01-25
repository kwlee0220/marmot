package marmot.optor;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Partitioner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import marmot.mapreduce.MarmotMapOutputKeyColumns;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapReduceJoint {
	private FOption<MarmotMapOutputKeyColumns> m_mapOutputKey = FOption.empty();
	private int m_nreducers;
	@SuppressWarnings("rawtypes")
	private FOption<Class<? extends Partitioner>> m_partionerClass = FOption.empty();
	
	private final List<RecordSetOperator> m_mapOptors = Lists.newArrayList();
	private final List<RecordSetOperator> m_combineOptors = Lists.newArrayList();
	private final List<RecordSetOperator> m_reduceOptors = Lists.newArrayList();
	
	public static MapReduceJoint create() {
		return new MapReduceJoint();
	}
	
	private MapReduceJoint() {
	}
	
	public FOption<MarmotMapOutputKeyColumns> getMapOutputKey() {
		return m_mapOutputKey;
	}
	
	public MapReduceJoint setMapOutputKey(MarmotMapOutputKeyColumns key) {
		m_mapOutputKey = FOption.of(key);
		return this;
	}
	
	public int getReducerCount() {
		return m_nreducers;
	}
	
	public MapReduceJoint setReducerCount(int count) {
		Preconditions.checkArgument(count > 0, "reducer count is invalid: count=" + count);
		
		m_nreducers = count;
		return this;
	}
	
	@SuppressWarnings("rawtypes")
	public FOption<Class<? extends Partitioner>> getPartitionerClass() {
		return m_partionerClass;
	}
	
	@SuppressWarnings("rawtypes")
	public MapReduceJoint setPartitionerClass(Class<? extends Partitioner> cls) {
		m_partionerClass = FOption.of(cls);
		return this;
	}
	
	public List<RecordSetOperator> getMapperAll() {
		return m_mapOptors;
	}
	
	public MapReduceJoint addMapper(RecordSetOperator... optors) {
		m_mapOptors.addAll(Arrays.asList(optors));
		return this;
	}
	
	public List<RecordSetOperator> getCombinerAll() {
		return m_combineOptors;
	}
	
	public MapReduceJoint addCombiner(RecordSetOperator... optors) {
		m_combineOptors.addAll(Arrays.asList(optors));
		return this;
	}
	
	public List<RecordSetOperator> getReducerAll() {
		return m_reduceOptors;
	}
	
	public MapReduceJoint addReducer(RecordSetOperator... optors) {
		m_reduceOptors.addAll(Arrays.asList(optors));
		return this;
	}
}