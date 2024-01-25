package marmot.mapreduce.input.spatialjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import marmot.io.geo.index.GlobalIndexEntry;
import marmot.io.serializer.MarmotSerializers;
import marmot.optor.support.Match;
import utils.func.Lazy;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
class ClusterFileJoinSplit extends InputSplit implements Writable {
	private List<Match<GlobalIndexEntry>> m_matches;
	private Lazy<Long> m_length = Lazy.of(this::calcLength);
	
	ClusterFileJoinSplit() { }
	ClusterFileJoinSplit(List<Match<GlobalIndexEntry>> matches) {
		m_matches = matches;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return m_length.get();
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[]{};
	}
	
	public List<Match<GlobalIndexEntry>> getMatches() {
		return m_matches;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int count = MarmotSerializers.readVInt(in);
		m_matches = new ArrayList<>(count);
		for ( int i =0; i < count; ++i ) {
			GlobalIndexEntry left = GlobalIndexEntry.deserialize(in);
			GlobalIndexEntry right = GlobalIndexEntry.deserialize(in);
			m_matches.add(new Match<>(left, right));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		MarmotSerializers.writeVInt(m_matches.size(), out);
		for ( Match<GlobalIndexEntry> match: m_matches ) {
			match.m_left.serialize(out);
			match.m_right.serialize(out);
		}
	}
	
	@Override
	public String toString() {
		return FStream.from(m_matches)
					.map(m -> String.format("(%s<->%s)", m.m_left.quadKey(), m.m_right.quadKey()))
					.join(",");
	}
	
	private long calcLength() {
		return FStream.from(m_matches)
						.mapToLong(m -> m.m_left.length() + m.m_right.length())
						.sum();
	}
}