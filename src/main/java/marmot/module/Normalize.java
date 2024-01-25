package marmot.module;

import static marmot.optor.AggregateFunction.MAX;
import static marmot.optor.AggregateFunction.MIN;
import static marmot.optor.StoreDataSetOptions.FORCE;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import utils.Utilities;
import utils.func.FOption;
import utils.stream.FStream;

import marmot.Column;
import marmot.ExecutePlanOptions;
import marmot.MarmotCore;
import marmot.Plan;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.analysis.module.NormalizeParameters;
import marmot.dataset.DataSet;
import marmot.optor.AggregateFunction;
import marmot.optor.StoreDataSetOptions;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Normalize implements MarmotModule {
	@SuppressWarnings("unused")
	private static final Logger s_logger = LoggerFactory.getLogger(Normalize.class);
	public static final String NAME = "normalize";

	private MarmotCore m_marmot;
	private NormalizeParameters m_params;

	@Override
	public List<String> getParameterNameAll() {
		return NormalizeParameters.getParameterNameAll();
	}

	@Override
	public RecordSchema getOutputRecordSchema(MarmotCore marmot,
												Map<String, String> paramsMap) {
		NormalizeParameters params = NormalizeParameters.fromMap(paramsMap);
		DataSet input = marmot.getDataSet(params.inputDataset());
			
		RecordSchema.Builder builder = input.getRecordSchema().toBuilder();
		params.inputFeatureColumns()
				.stream()
				.forEach(name -> builder.addOrReplaceColumn(name, DataType.DOUBLE));
		return builder.build();
	}

	@Override
	public void initialize(MarmotCore marmot, Map<String, String> paramsMap) {
		m_marmot = marmot;
		m_params = NormalizeParameters.fromMap(paramsMap);
		
		Utilities.checkNotNullArgument(m_params.inputDataset(), "parameter is missing: " + NormalizeParameters.INPUT_DATASET);
		Utilities.checkNotNullArgument(m_params.outputDataset(), "parameter is missing: " + NormalizeParameters.OUTPUT_DATASET);
		Utilities.checkNotNullArgument(m_params.inputFeatureColumns(), "parameter is missing: " + NormalizeParameters.INPUT_FEATURE_COLUMNS);
		Utilities.checkNotNullArgument(m_params.outputFeatureColumns(), "parameter is missing: " + NormalizeParameters.OUTPUT_COLUMNS);
	}

	@Override
	public void run() {
		List<String> inCols = m_params.inputFeatureColumns();
		List<String> outCols = m_params.outputFeatureColumns();
		
		DataSet input = m_marmot.getDataSet(m_params.inputDataset());
		RecordSchema schema = input.getRecordSchema();
		
		List<String> invalidCols = Lists.newArrayList();
		for ( String colName: m_params.inputFeatureColumns() ) {
			FOption<Column> col = schema.findColumn(colName);
			if ( col.isAbsent() ) {
				invalidCols.add(colName);
			}
			else {
				DataType type = col.getUnchecked().type();
				switch ( type.getTypeCode() ) {
					case DOUBLE: case LONG: case INT: case FLOAT: case SHORT: case BYTE:
						break;
					default:
						invalidCols.add(colName);
				}
			}
		}
		if ( invalidCols.size() > 0 ) {
			String colStr = FStream.from(invalidCols)
									.map(schema::getColumn)
									.map(Column::toString)
									.join(',');
			String msg = String.format("process[%s]: unknown input features: %s", NAME, colStr);
			throw new IllegalArgumentException(msg);
		}

		String colDecls = FStream.from(inCols).map(name -> name + ":double").join(",");
		AggregateFunction[] aggrs = FStream.from(inCols)
											.flatMap(name -> FStream.of(MIN(name).as(name + "_min"),
																		MAX(name).as(name + "_max")))
											.toArray(AggregateFunction.class);
		Path tmpFile = new Path("tmp/" + UUID.randomUUID());
		try {
			Plan plan;
			plan = Plan.builder("calc_statistics")
							.load(m_params.inputDataset())
							.expand(colDecls)
							.aggregate(aggrs)
							.build();
			Record stats = m_marmot.executeToRecord(plan).get();
			
			String initExpr1 = FStream.of(stats.getAll())
									.buffer(2, 2)
									.map(pair -> (double)pair.get(1) - (double)pair.get(0))
									.map(Object::toString)
									.join(",", "$total = [", "]");
			String initExpr2 = FStream.of(stats.getAll())
									.buffer(2, 2)
									.map(pair -> (double)pair.get(0))
									.map(Object::toString)
									.join(",", "$min = [", "]");
			String initExpr = initExpr1 + "; " + initExpr2;

			String updExpr = FStream.from(outCols).zipWithIndex()
									.map(t -> String.format("%s = (%s-$min[%d])/$total[%d];",
														outCols.get(t._2), inCols.get(t._2), t._2, t._2))
									.join(" ");
			String outColDecls = FStream.from(outCols).map(name -> name + ":double").join(",");
			
			StoreDataSetOptions opts = input.hasGeometryColumn()
										? FORCE(input.getGeometryColumnInfo())
										: FORCE;
			plan = Plan.builder("attach_portion")
							.load(m_params.inputDataset())
							.expand(colDecls)
							.expand(outColDecls)
							.update(RecordScript.of(initExpr, updExpr))
							.store(m_params.outputDataset(), opts)
							.build();
			m_marmot.execute(plan, ExecutePlanOptions.DEFAULT);
		}
		finally {
			m_marmot.getFileServer().deleteFile(tmpFile);
		}
	}
	
	@Override
	public String toString() {
		String inColExpr = m_params.inputFeatureColumns().stream()
									.collect(Collectors.joining(","));
		String outColExpr = m_params.outputFeatureColumns().stream()
									.collect(Collectors.joining(","));
		
		return String.format("Normalize(%s{%s})->%s{%s}",
							m_params.inputDataset(), inColExpr,
							m_params.outputDataset(), outColExpr);
	}
}
