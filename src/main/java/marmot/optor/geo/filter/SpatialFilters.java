package marmot.optor.geo.filter;

import marmot.plan.PredicateOptions;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SpatialFilters {
	public static final DropEmptyGeometry skipEmptyGeometry(String geomCol) {
		return new DropEmptyGeometry(geomCol, PredicateOptions.DEFAULT);
	}
	
	public static final DropEmptyGeometry skipNonEmptyGeometry(String geomCol) {
		return new DropEmptyGeometry(geomCol, PredicateOptions.NEGATED);
	}
}
