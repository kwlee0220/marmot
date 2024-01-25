package marmot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.Maps;

import marmot.dataset.DataSet;
import marmot.proto.optor.PlanProto;
import marmot.support.DefaultRecord;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class TestUtils {
	public static Plan toProtoAndFromIt(Plan  plan) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		plan.toProto().writeTo(baos);
		baos.close();
		
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		return Plan.fromProto(PlanProto.parseFrom(bais));
	}
	
	public static void printPrefix(DataSet dataset, int count) {
		try ( RecordSet rset = dataset.read() ) {
			printPrefix(dataset.read(), count);
		}
	}
	
	public static void printPrefix(RecordSet rset, int count) {
		RecordSchema schema = rset.getRecordSchema();
		Record record = DefaultRecord.of(schema);
		int[] colIdxs = schema.getColumns().stream()
							.filter(c -> !c.type().isGeometryType())
							.mapToInt(c -> c.ordinal())
							.toArray();
		
		int i = 0;
		try {
			while ( ++i <= count && rset.next(record) ) {
				Map<String,Object> values = Maps.newHashMap();
				for ( int j =0; j < colIdxs.length; ++j ) {
					String name = schema.getColumnAt(colIdxs[j]).name();
					Object value = record.get(colIdxs[j]);
					values.put(name, value);
				}
				System.out.println(values);
			}
		}
		finally {
			rset.closeQuietly();
		}
	}
}
