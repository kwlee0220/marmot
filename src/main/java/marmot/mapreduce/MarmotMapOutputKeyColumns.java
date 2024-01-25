package marmot.mapreduce;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import marmot.RecordSchema;
import marmot.io.MultiColumnKey;
import marmot.io.RecordWritable;
import marmot.optor.KeyColumn;
import marmot.optor.SortOrder;
import marmot.support.DataUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class MarmotMapOutputKeyColumns implements MapReduceJobConfigurer {
	private final MultiColumnKey m_keyCols;
	private final int m_groupKeyLength;
	
	public static MarmotMapOutputKeyColumns fromGroupKey(MultiColumnKey groupKey) {
		return new MarmotMapOutputKeyColumns(groupKey, groupKey.length());
	}
	
	public static MarmotMapOutputKeyColumns fromGroupKey(MultiColumnKey groupKey,
															MultiColumnKey sortKey) {
		MultiColumnKey keyCols = MultiColumnKey.concat(groupKey, sortKey);
		
		return new MarmotMapOutputKeyColumns(keyCols, groupKey.length());
	}
	
	public static MarmotMapOutputKeyColumns fromSortKey(MultiColumnKey sortKey) {
		return new MarmotMapOutputKeyColumns(sortKey, 0);
	}
	
	private MarmotMapOutputKeyColumns(MultiColumnKey keyCols, int groupKeyLength) {
		m_keyCols = keyCols;
		m_groupKeyLength = groupKeyLength;
	}
	
	public MarmotMapOutputKey create(RecordSchema mapOutputRecordSchema) {
		return new MarmotMapOutputKey(m_groupKeyLength, m_keyCols, mapOutputRecordSchema);
	}
	
	public int length() {
		return m_keyCols.length();
	}

	@Override
	public void configure(Job job) {
		job.setMapOutputKeyClass(MarmotMapOutputKey.class);
		job.getConfiguration().set(MapReduceStage.PROP_MAP_OUTPUT_KEY, toString());
		
		if ( isGroupKey() ) {
			job.setPartitionerClass(GroupPartitioner.class);
			job.setGroupingComparatorClass(GroupComparator.class);
		}
		
		job.setSortComparatorClass(SortComparator.class);
	}
	
	public static MarmotMapOutputKeyColumns fromString(String str) {
		MultiColumnKey keyCols = MultiColumnKey.fromString(str);
		long grpKeyCount = keyCols.streamKeyColumns()
									.map(KeyColumn::sortOrder)
									.filter(order -> order == SortOrder.NONE)
									.count();
		
		return new MarmotMapOutputKeyColumns(keyCols, (int)grpKeyCount);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for ( int i =0; i < m_keyCols.length(); ++i ) {
			KeyColumn kc = m_keyCols.getKeyColumnAt(i);
			builder.append(kc.name());
			if ( i >= m_groupKeyLength ) {
				builder.append(':').append(kc.sortOrder())
						.append(':').append(kc.nullsOrder());
			}
			builder.append(',');
		}
		builder.setLength(builder.length()-1);
		
		return builder.toString();
	}
	
	private boolean isGroupKey() {
		return m_groupKeyLength > 0;
	}

	public static class GroupPartitioner extends Partitioner<MarmotMapOutputKey,RecordWritable> {
		@Override
		public int getPartition(MarmotMapOutputKey key, RecordWritable value, int numPartitions) {
			return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
		}
	}

	public static final String CONFIG_KEY_PARTITION_INDEX_COLUMN_INDEX
													= "marmot.mapreduce.partition.column_index";
	public static class ValueBasedGroupPartitioner
										extends Partitioner<MarmotMapOutputKey,RecordWritable>
										implements Configurable {
		private Configuration m_conf;
		private int m_colIdx;
		
		public static void setPartitionColumnIndex(Configuration conf, int colIdx) {
			conf.setInt(CONFIG_KEY_PARTITION_INDEX_COLUMN_INDEX, colIdx);
		}
		
		@Override
		public int getPartition(MarmotMapOutputKey key, RecordWritable value, int numPartitions) {
			int partIdx = DataUtils.asInt(value.get(m_colIdx));
			
			return (partIdx & Integer.MAX_VALUE) % numPartitions;
		}

		@Override
		public Configuration getConf() {
			return m_conf;
		}

		@Override
		public void setConf(Configuration conf) {
			m_colIdx = conf.getInt(CONFIG_KEY_PARTITION_INDEX_COLUMN_INDEX, -1);
			if ( m_colIdx < 0 ) {
				throw new IllegalArgumentException("Configuration does not have partition.key.column");
			}
		}
	}
	
	public static class GroupComparator extends WritableComparator {
		public GroupComparator() {
			super(MarmotMapOutputKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((MarmotMapOutputKey)w1).compareGroupTo((MarmotMapOutputKey)w2);
		}
	}
	
	public static class SortComparator extends WritableComparator {
		public SortComparator() {
			super(MarmotMapOutputKey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			return ((MarmotMapOutputKey)w1).compareTo((MarmotMapOutputKey)w2);
		}
	}
}
