package marmot.exec;

import marmot.MarmotInternalException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class AnalysisStoreException extends MarmotInternalException {
	private static final long serialVersionUID = -1686456883689347103L;

	public AnalysisStoreException(String details) {
		super(details);
	}

	public AnalysisStoreException(Throwable cause) {
		super(cause);
	}
}
