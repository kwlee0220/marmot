package marmot.mapreduce.support;

import marmot.RecordSetException;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MapTaskStoppedException extends RecordSetException {
	private static final long serialVersionUID = 3988825790519059272L;

	public MapTaskStoppedException(String details) {
		super(details);
	}
}
