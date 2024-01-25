package marmot.optor.geo.join;

import static marmot.optor.geo.SpatialRelation.WITHIN_DISTANCE;

import org.locationtech.jts.geom.Geometry;

import com.google.common.base.Preconditions;

import marmot.Record;
import marmot.RecordSet;
import marmot.plan.SpatialJoinOptions;
import utils.UnitUtils;
import utils.func.FOption;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class SpatialKnnJoin<T extends SpatialKnnJoin<T>>
													extends NestedLoopSpatialJoin<T> {
	private final double m_radius;
	private final FOption<Integer> m_topK;

	protected abstract RecordSet handleKnnMatches(NestedLoopKnnMatch match);
	
	protected SpatialKnnJoin(String geomCol, String paramDsId, double radius, FOption<Integer> topK,
							SpatialJoinOptions opts) {
		super(geomCol, paramDsId, opts.joinExpr(WITHIN_DISTANCE(radius)));
		Preconditions.checkArgument(Double.compare(radius, 0) >= 0);
		
		m_radius = radius;
		m_topK = topK;
	}
	
	public final double getRadius() {
		return m_radius;
	}
	
	public final FOption<Integer> getTopK() {
		return m_topK;
	}

	@Override
	protected final RecordSet doInnerLoop(NestedLoopMatch match) {
		Geometry outer = getOuterGeometry(match.getOuterRecord());
		
		// 만일 'top-k'가 지정된 경우에는 
		// Outer record과 조인 관계가 있는 모든 inner record들 사이의 거리를
		// 계산하여 가장 가까운 순서대로 정렬한 후, 주어진 k개 만큼만 선택한다.
		FStream<Tuple<Record,Double>> matcheds
							= match.getInnerRecords()
									.map(inner -> {
										Geometry innerGeom = getInnerGeometry(inner);
										double dist = outer.distance(innerGeom);
							
										return Tuple.of(inner, dist);
									});
		if ( getTopK().isPresent() ) {
			matcheds = matcheds.takeTopK(getTopK().get(),
										(t1,t2) -> Double.compare(t1._2, t2._2));
		}
		
		NestedLoopKnnMatch knnMatch = new NestedLoopKnnMatch(match.getOuterRecord(),
															match.getInnerRecordSchema(),
															matcheds);
		return handleKnnMatches(knnMatch);
	}
	
	@Override
	public String toString() {
		String topKStr = m_topK.map(k -> String.format(", k=%d", k))
								.getOrElse("");
		return String.format("spatial_knn_join[r=%s%s]", UnitUtils.toMeterString(m_radius), topKStr);
	}
}
