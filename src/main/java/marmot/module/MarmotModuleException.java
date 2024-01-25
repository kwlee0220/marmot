package marmot.module;

import marmot.MarmotRuntimeException;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class MarmotModuleException extends MarmotRuntimeException {
	private static final long serialVersionUID = 1330197553023786481L;

	public MarmotModuleException(String details) {
		super(details);
	}
}
