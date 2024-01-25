package marmot.optor.geo.transform;

import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.precision.GeometryPrecisionReducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import marmot.support.GeoFunctions;
import marmot.support.GeoUtils;
import marmot.type.GeometryDataType;
import utils.func.FOption;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BinarySpatialDifference extends BinarySpatialTransform {
	private static final Logger s_logger = LoggerFactory.getLogger(BinarySpatialDifference.class);

	public BinarySpatialDifference(String leftGeomCol, String rightGeomCol, String outputGeomCol,
									FOption<GeometryDataType> outputGeomType) {
		super(leftGeomCol, rightGeomCol, outputGeomCol, outputGeomType);
		
		setLogger(s_logger);
	}
	
	@Override
	protected Geometry transform(Geometry left, Geometry right) {
		if ( GeoFunctions.ST_IsEmpty(left) ) {
			return null;
		}
		else if ( GeoFunctions.ST_IsEmpty(right) ) {
			return left;
		}
		
		try {
			return left.difference(right);
		}
		catch ( Exception e ) {
			PrecisionModel model = new PrecisionModel(PrecisionModel.FLOATING_SINGLE);
			GeometryPrecisionReducer reducer = new GeometryPrecisionReducer(model);
			
			List<Polygon> lefts = GeoUtils.flatten(left, Polygon.class);
			List<Polygon> rights = GeoUtils.flatten(right, Polygon.class);
			
			List<Polygon> collecteds = Lists.newArrayList();
			for ( Polygon leftP: lefts ) {
				for ( Polygon rightP: rights ) {
					try {
						Geometry leftP2 = reducer.reduce(leftP);
						Geometry rightP2 = reducer.reduce(rightP);
						Geometry result = leftP2.intersection(rightP2);
						if ( result instanceof Polygon ) {
							collecteds.add((Polygon)result);
						}
					}
					catch ( Throwable ignored ) {
						System.out.println(ignored);
					}
				}
			}
			
			return GeoUtils.toMultiPolygon(collecteds);
		}
	}
}
