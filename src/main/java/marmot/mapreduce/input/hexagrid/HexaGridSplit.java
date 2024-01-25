package marmot.mapreduce.input.hexagrid;

import static marmot.io.serializer.MarmotSerializers.ENVELOPE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.grid.Grids;
import org.locationtech.jts.geom.Envelope;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import marmot.Record;
import marmot.RecordSet;
import marmot.geo.CRSUtils;
import marmot.geo.geotools.SimpleFeatures;
import marmot.optor.rset.GridRecordSet;
import marmot.support.DefaultRecord;
import marmot.type.GridCell;
import utils.stream.FStream;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HexaGridSplit extends InputSplit implements Writable {
	private static final String[] EMPTY_HOSTS = new String[0];

	private int m_id;
	private int m_ncols;
	private String m_srid;
	private Envelope m_bounds;
	private Envelope m_region;
	private double m_sideLen;
	
	HexaGridSplit() { }
	
	public HexaGridSplit(int id, int ncols, String srid, Envelope bounds, Envelope region,
						double sideLen) {
		m_id = id;
		m_ncols = ncols;
		m_srid = srid;
		m_bounds = bounds;
		m_region = region;
		m_sideLen = sideLen;
	}
	
	public RecordSet getRecordSet() {
		CoordinateReferenceSystem crs = CRSUtils.toCRS(m_srid);
		ReferencedEnvelope univ = new ReferencedEnvelope(m_bounds, crs);
		SimpleFeatureSource univGrid = Grids.createHexagonalGrid(univ,m_sideLen);
		
		FStream<Record> strm = SimpleFeatures.toRecordSet(univGrid)
								.fstream()
								.filter(r -> m_region.contains(r.getGeometry(0)
													.getCentroid().getCoordinate()))
								.map(r -> {
									long ordinal = r.getInt(1)-1;
									
									Record o = DefaultRecord.of(GridRecordSet.SCHEMA);
									o.set(0, r.get(0));
									o.set(1, new GridCell((int)(ordinal%m_ncols),
															(int)(ordinal/m_ncols)));
									o.set(2, ordinal);
									return o;
								});
		return RecordSet.from(GridRecordSet.SCHEMA, strm.iterator());
	}
	
	int getPartitionId() {
		return m_id;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return Math.round(m_region.getArea());
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return EMPTY_HOSTS;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		m_id = in.readInt();
		m_srid = in.readUTF();
		m_bounds = ENVELOPE.deserialize(in);
		m_region = ENVELOPE.deserialize(in);
		m_sideLen = in.readDouble();
		m_ncols = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(m_id);
		out.writeUTF(m_srid);
		ENVELOPE.serialize(m_bounds, out);
		ENVELOPE.serialize(m_region, out);
		out.writeDouble(m_sideLen);
		out.writeInt(m_ncols);
	}
	
	@Override
	public String toString() {
		int row = m_id / m_ncols;
		int col = m_id % m_ncols;
		return String.format("(%d,%d): (%.0fx%.0f)",
							col, row, m_region.getWidth(), m_region.getHeight());
	}
}