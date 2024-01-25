package marmot.mapreduce.support;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public final class ClusterNodeInfo {
	public enum State {
		NEW, RUNNING, UNHEALTHY, DECOMMISSIONING, DECOMMISSIONED, LOST, REBOOTED,
	};

	private final String m_id;
	private final String m_rack;
	private final String m_hostName;
	private final State m_state;
	private final int m_numContainers;
	private final int m_coreCount;
	
	ClusterNodeInfo(String id, String rack, String hostName, State state,
					long numContainers, long coreCount) {
		m_id = id;
		m_rack = rack;
		m_hostName = hostName;
		m_state = state;
		m_numContainers = (int)numContainers;
		m_coreCount = (int)coreCount;
	}
	
	public String id() {
		return m_id;
	}
	
	public String rack() {
		return m_rack;
	}
	
	public String hostname() {
		return m_hostName;
	}
	
	public State state() {
		return m_state;
	}
	
	public int coreCount() {
		return m_coreCount;
	}
	
	@Override
	public String toString() {
		return String.format("%s(%s:%d)", m_id, m_state, m_coreCount);
	}
}
