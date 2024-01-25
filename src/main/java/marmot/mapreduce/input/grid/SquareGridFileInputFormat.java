package marmot.mapreduce.input.grid;

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

import marmot.io.RecordWritable;
import marmot.io.serializer.MarmotSerializers;
import utils.Size2d;
import utils.Size2i;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class SquareGridFileInputFormat extends FileInputFormat<NullWritable, RecordWritable> {
	private static final String PROP_PARAMETER = "marmot.mapreduce.input.grid.parameter";
	
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		Configuration conf = job.getConfiguration();
		
		final Parameters params = getParameters(conf);

		Envelope bounds = params.getGridBounds();
		Size2d cellSize = params.getCellSize();
		int nmappers = params.getSplitCount();
		
		// Mapper 갯수를 이용하여 각 mapper가 담당할 grid 구역을 산정한다.
		int sideLength = (int)Math.ceil(Math.sqrt(nmappers));
		List<InputSplit> splitList = Lists.newArrayList();
		int id = 0;
		for ( int row =0; row < sideLength; ++row ) {
			for ( int col =0; col < sideLength; ++col ) {
				Size2i partitionDim = new Size2i(sideLength, sideLength);
				splitList.add(new SquareGridSplit(bounds, id, partitionDim, cellSize));
				++id;
			}
		}
		
		return splitList;
	}

	@Override
	public RecordReader<NullWritable,RecordWritable> createRecordReader(InputSplit split,
																TaskAttemptContext context) {
		return new SquareGridRecordReader();
	}
	
	public static final Parameters getParameters(Configuration conf) {
		String str = conf.get(PROP_PARAMETER);
		if ( str == null ) {
			throw new IllegalStateException(SquareGridFileInputFormat.class
											+ " does not have its parameter");
		}

		return MarmotSerializers.fromBase64String(str, Parameters::deserialize);
	}
	
	public static void setParameters(Configuration conf, Parameters params) {
		conf.set(PROP_PARAMETER, MarmotSerializers.toBase64String(params));
	}
}
