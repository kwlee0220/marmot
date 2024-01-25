package marmot.optor.geo.advanced;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public enum ClusterType {
	HH, LL, LH, HL, OUTLIER;
	
	public static ClusterType fromZValue(double x, double zvalue, double avg) {
		int cmp = Double.compare(zvalue, 1.96);
		if ( cmp > 0 ) {
			return (Double.compare(x - avg, 0) >= 0) ? HH : LL;
		}
		else if ( cmp < 0 ) {
			return (Double.compare(x - avg, 0) >= 0) ? LH : HL;
		}
		else {
			return OUTLIER;
		}
	}
}
