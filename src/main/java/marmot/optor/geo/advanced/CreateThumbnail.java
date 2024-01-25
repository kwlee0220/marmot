package marmot.optor.geo.advanced;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSets;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.proto.optor.BuildThumbnailProto;
import marmot.rset.PeekableRecordSet;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.func.Tuple;
import utils.stream.FStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CreateThumbnail extends AbstractRecordSetFunction
						implements PBSerializable<BuildThumbnailProto> {
	private final String m_geomCol;
	private final double m_ratio;
	
	public CreateThumbnail(String geomCol, double ratio) {
		m_geomCol = geomCol;
		m_ratio = ratio;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		if ( !inputSchema.getColumn(m_geomCol).type().isGeometryType() ) {
			throw new IllegalArgumentException("not geometry column: name=" + m_geomCol);
		}
		
		RecordSchema outSchema = inputSchema.toBuilder()
											.removeColumn("__count")
											.build();
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	@Override
	public RecordSet apply(RecordSet input) {
		PeekableRecordSet prset = RecordSets.toPeekable(input);
		
		Record first = prset.peek().get();
		
		int nrecords = first.getInt("__count");
		int nsamples = (int)Math.max(1, Math.round(nrecords * m_ratio));

		FStream<Record> selecteds;
		DataType geomType = input.getRecordSchema().getColumn(m_geomCol).type();
		switch ( geomType.getTypeCode() ) {
			case POLYGON:
			case MULTI_POLYGON:
				selecteds = selectByArea(input, nsamples);
				break;
			default:
				selecteds = selectByRandom(input, (int)nrecords);
				break;
		}
		
		return RecordSet.from(getRecordSchema(), selecteds);
	}
	
	private FStream<Record> selectByArea(RecordSet input, int nsamples) {
		return input.fstream()
					.map(r -> Tuple.of(r, r.getGeometry(m_geomCol).getArea()))
					.takeTopK(nsamples, (t1,t2) -> Double.compare(t2._2, t1._2))
					.map(t -> t._1);
	}
	
	private FStream<Record> selectByRandom(RecordSet input, int nrecords) {
		return input.fstream()
					.sample(nrecords, m_ratio)
					.shuffle();
	}
	
	@Override
	public String toString() {
		return String.format("build_thumbnail[ratio=%.2f]", m_ratio);
	}

	public static CreateThumbnail fromProto(BuildThumbnailProto proto) {
		return new CreateThumbnail(proto.getGeomColumn(), proto.getSampleRatio());
	}

	@Override
	public BuildThumbnailProto toProto() {
		return BuildThumbnailProto.newBuilder()
							.setGeomColumn(m_geomCol)
							.setSampleRatio(m_ratio)
							.build();
	}
}
