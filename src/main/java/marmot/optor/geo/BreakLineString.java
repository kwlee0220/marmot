package marmot.optor.geo;

import java.util.List;
import java.util.function.Function;

import org.locationtech.jts.geom.LineString;

import com.google.common.collect.Lists;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.geo.GeoClientUtils;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.BreakLineStringProto;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Utilities;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class BreakLineString extends AbstractRecordSetFunction
							implements PBSerializable<BreakLineStringProto> {
	private final String m_geomCol;
	private int m_geomColIdx = -1;
	
	public BreakLineString(String geomColName) {
		Utilities.checkNotNullArgument(geomColName, "Geometry column");
		
		m_geomCol = geomColName;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		m_marmot = marmot;
		RecordSchema outSchema = inputSchema;
		
		Column geomCol = outSchema.getColumn(m_geomCol);
		if ( !DataType.LINESTRING.equals(geomCol.type()) ) {
			throw new IllegalArgumentException("Geometry column is not LineString: col=" + m_geomCol);
		}
		m_geomColIdx = geomCol.ordinal();
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return RecordSet.from(getRecordSchema(), input.fstream().flatMap(m_mapper));
	}
	
	@Override
	public String toString() {
		return String.format("break_linestring[%s]", m_geomCol);
	}
	
	public static BreakLineString fromProto(BreakLineStringProto proto) {
		return new BreakLineString(proto.getGeometryColumn());
	}

	@Override
	public BreakLineStringProto toProto() {
		return BreakLineStringProto.newBuilder()
									.setGeometryColumn(m_geomCol)
									.build();
	}
	
	private Function<Record,FStream<Record>> m_mapper = (r) -> {
		LineString geom = (LineString)r.getGeometry(m_geomColIdx);
		return FStream.from(breakLineString(geom))
					.map(line -> {
						Record copy = r.duplicate();
						copy.set(m_geomColIdx, line);
						return copy;
					});
	};
	
	private List<LineString> breakLineString(LineString lstr) {
		List<LineString> frags = Lists.newArrayList();
		for ( int i = 0; i < lstr.getNumPoints(); ++i ) {
			if ( i > 0 ) {
				frags.add(GeoClientUtils.toLineString(lstr.getPointN(i-1).getCoordinate(),
													lstr.getPointN(i).getCoordinate()));
			}
		}
		
		return frags;
	}
}