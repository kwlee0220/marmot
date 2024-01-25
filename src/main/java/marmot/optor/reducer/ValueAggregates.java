package marmot.optor.reducer;

import java.lang.reflect.Constructor;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.optor.AggregateType;
import marmot.optor.geo.reducer.AggrConvexHull;
import marmot.optor.geo.reducer.AggrEnvelope;
import marmot.optor.geo.reducer.AggrUnionGeom;
import marmot.support.PBException;
import utils.CSV;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ValueAggregates {
    private static final Map<String,Class<? extends ValueAggregate>> AGGREGATES = Maps.newHashMap();
    static {
        AGGREGATES.put(AggregateType.COUNT.getName(), Count.class);
        AGGREGATES.put(AggregateType.MAX.getName(), Max.class);
        AGGREGATES.put(AggregateType.MIN.getName(), Min.class);
        AGGREGATES.put(AggregateType.SUM.getName(), Sum.class);
        AGGREGATES.put(AggregateType.AVG.getName(), Avg.class);
        AGGREGATES.put(AggregateType.STDDEV.getName(), StdDev.class);
        AGGREGATES.put(AggregateType.CONVEX_HULL.getName(), AggrConvexHull.class);
        AGGREGATES.put(AggregateType.ENVELOPE.getName(), AggrEnvelope.class);
        AGGREGATES.put(AggregateType.UNION_GEOM.getName(), AggrUnionGeom.class);
        AGGREGATES.put(AggregateType.CONCAT_STR.getName(), ConcatString.class);
    }
    
    static String toProto(ValueAggregate aggr) {
    	String aggrCol = ( aggr.getAggregateColumn() != null )
    					? aggr.getAggregateColumn() : "";
		String header = String.format("%s:%s:%s", aggr.getName().toLowerCase(),
										aggrCol, aggr.getOutputColumn());
		return aggr.getParameter()
					.map(param -> header + "?" + param)
					.getOrElse(header);
    }
    
    static ValueAggregate fromProto(String proto) {
    	String[] parts = CSV.parseCsvAsArray(proto, '?', '\\');
    	String[] comps = CSV.parseCsvAsArray(parts[0], ':', '\\');
    	if ( comps.length != 3 ) {
    		throw new IllegalArgumentException("invalid AggregateFunction: str=" + proto);
    	}
    	
    	String aggrName = comps[0].trim().toLowerCase();
    	String aggrCol = comps[1].trim();
    	if ( aggrCol.length() == 0 ) {
    		aggrCol = null;
    	}
    	String outCol = comps[2].trim();
    	String args = (parts.length > 1) ? parts[1] : null;

        try {
            Class<? extends ValueAggregate> aggrCls = AGGREGATES.get(aggrName);
            ValueAggregate aggr;
            if ( args != null ) {
            	Constructor<? extends ValueAggregate> ctor
            					= aggrCls.getConstructor(String.class, String.class);
                aggr = ctor.newInstance(aggrCol, args);
            }
            else {
            	Constructor<? extends ValueAggregate> ctor = aggrCls.getConstructor(String.class);
                aggr = ctor.newInstance(aggrCol);
            }
            if ( outCol != null ) {
                aggr.setOutputColumn(outCol);
            }

            return aggr;
        }
        catch ( Exception e ) {
            throw new PBException("fails to load ValueAggregator: proto=" + proto + ", cause" + e);
        }
    }
}
