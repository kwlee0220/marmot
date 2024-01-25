package marmot.module.geo.arc;

import utils.CSV;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ArcGisUtils {
	private ArcGisUtils() {
		throw new AssertionError("Should not be called: class=" + getClass());
	}
	
	public static String combineColumns(String leftCols, String rightCols) {
		rightCols = CSV.parseCsv(rightCols)
						.map(n -> {
							String suffixed = (n.length() > 8) ? n.substring(0, 8) : n;
							return String.format("%s as %s_1", n, suffixed);
						})
						.join(",");
		
		return String.format("left.{%s},right.{%s}", leftCols, rightCols);
	}
	
	public static String toProjectSuffixed(String cols) {
		return CSV.parseCsv(cols)
						.map(n -> {
							String suffixed = (n.length() > 8) ? n.substring(0, 8) : n;
							return String.format("%s as %s_1", n, suffixed);
						})
						.join(",");
	}
	
	public static String toSuffixed(String cols) {
		return CSV.parseCsv(cols)
						.map(n -> {
							String suffixed = (n.length() > 8) ? n.substring(0, 8) : n;
							return String.format("%s_1", suffixed);
						})
						.join(",");
	}
}
