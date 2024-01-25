package marmot.mapreduce.input.hexagrid;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.locationtech.jts.geom.Envelope;

import com.google.common.collect.Lists;

import marmot.MarmotCore;
import marmot.dataset.GeometryColumnInfo;
import marmot.geo.catalog.DataSetInfo;
import marmot.io.RecordWritable;
import marmot.type.DataType;
import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class HexagonGridFileInputFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final String PROP_GRID_BOUNDS = "marmot.geo.grid.universe";
	private static final String PROP_PARTION_DIMENSION = "marmot.geo.grid.partion.dimension";
	private static final String PROP_GRID_DATASET = "marmot.geo.grid.dataset";
	private static final String PROP_GRID_SRID = "marmot.geo.grid.srid";
	private static final String PROP_GRID_SIDE_LENGTH = "marmot.geo.grid.side_length";

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();

		String srid = getSRID(conf);
		Envelope bounds = getGridBounds(conf);
		String dataset = getDataSetId(conf);
		if ( dataset != null ) {
			MarmotCore marmot = new MarmotCore(job);
			DataSetInfo info = marmot.getDataSetInfo(dataset);
			srid = info.getGeometryColumnInfo()
						.map(GeometryColumnInfo::srid)
						.getOrElse(srid);
			bounds = info.getBounds();
		}
		Size2i partDim = getPartionDimension(conf);
		double sideLength = getSideLength(conf);
		
		double x0 = bounds.getMinX();
		double y0 = bounds.getMinY();
		double partWidth = bounds.getWidth() / partDim.getWidth();
		double partHeight = bounds.getHeight() / partDim.getHeight();
		
		List<InputSplit> splitList = Lists.newArrayList();
		int id = 0;
		for ( int row =0; row < partDim.getHeight(); ++row ) {
			double y1 = y0 + row * partHeight;
			double y2 = y1 + partHeight;
			
			for ( int col =0; col < partDim.getWidth(); ++col ) {
				double x1 = x0 + col * partWidth;
				double x2 = x1 + partWidth;
				
				Envelope region = new Envelope(x1, x2, y1, y2);
				splitList.add(new HexaGridSplit(id, partDim.getWidth(), srid, bounds, region, sideLength));
				++id;
			}
		}
		
		return splitList;
	}

	@Override
	public RecordReader<NullWritable,RecordWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context) {
		return new HexaGridRecordReader();
	}
	
	public static Envelope getGridBounds(Configuration conf) {
		String wkt = conf.get(PROP_GRID_BOUNDS);
		return (wkt != null) ? DataType.ENVELOPE.parseInstance(wkt) : null;
	}
	
	public static void setGridBounds(Configuration conf, Envelope envl) {
		conf.set(PROP_GRID_BOUNDS, DataType.ENVELOPE.toInstanceString(envl));
	}
	
	public static Size2i getPartionDimension(Configuration conf) {
		String str = conf.get(PROP_PARTION_DIMENSION);
		
		return Size2i.fromString(str);
	}
	
	public static void setPartionDimension(Configuration conf, Size2i dim) {
		conf.set(PROP_PARTION_DIMENSION, dim.toString());
	}
	
	public static String getDataSetId(Configuration conf) {
		return conf.get(PROP_GRID_DATASET);
	}
	
	public static void setDataSetId(Configuration conf, String dataset) {
		conf.set(PROP_GRID_DATASET, dataset);
	}
	
	public static String getSRID(Configuration conf) {
		return conf.get(PROP_GRID_SRID);
	}
	
	public static void setSRID(Configuration conf, String srid) {
		conf.set(PROP_GRID_SRID, srid);
	}
	
	public static double getSideLength(Configuration conf) {
		String str = conf.get(PROP_GRID_SIDE_LENGTH);
		return Double.parseDouble(str);
	}
	
	public static void setSideLength(Configuration conf, double length) {
		conf.set(PROP_GRID_SIDE_LENGTH, ""+length);
	}
}
