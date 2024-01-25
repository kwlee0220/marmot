package marmot.optor;

import com.google.common.base.Preconditions;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.externio.csv.CsvUtils;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ParseCsvProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import marmot.type.DataType;
import utils.Throwables;
import utils.Utilities;


/**
 * {@code CsvTextRecordParser}은 텍스트 파일에 기록된 CSV 라인을 차례대로 읽어 파싱하여
 * 레코드로 구성하는 작업을 수행한다.
 * <p>
 * 하나의 텍스트 라인이 하나의 레코드로 구성되는 것을 가정한다.
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class ParseCsvText extends RecordLevelTransform
							implements PBSerializable<ParseCsvProto> {
	private final String m_csvColName;
	private final ParseCsvOptions m_options;
	private boolean m_trimColumn;
	
	private int m_csvColIdx;
	private CsvParser m_parser;
	private Column[] m_columns;
	
	private ParseCsvText(String csvColumn, ParseCsvOptions opts) {
		Utilities.checkNotNullArgument(csvColumn, "CSV column is null");
		Utilities.checkNotNullArgument(opts, "CsvOptions is null");
		Utilities.checkNotNullArgument(opts.header().isPresent(), "CSV header is null");

		m_csvColName = csvColumn;
		m_options = opts;
		
		m_trimColumn = opts.trimColumn().getOrElse(false);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
//		Utilities.checkNotNullArgument(marmot, "MarmotServer is null");
		
		Column col = inputSchema.findColumn(m_csvColName)
						.getOrThrow(() -> {
							String details = String.format("op=%s: input RecordSchema does not have "
															+ "'text' column", this);
							return new IllegalArgumentException(details);
						});
		Preconditions.checkArgument(col.type() == DataType.STRING,
									"invalid input RecordSchema");
		m_csvColIdx = col.ordinal();
		
		CsvParserSettings settings = new CsvParserSettings();
		m_options.nullValue().ifPresent(settings::setNullValue);
		
		CsvFormat format = settings.getFormat();
		format.setDelimiter(m_options.delimiter());
		m_options.quote().ifPresent(format::setQuote);
		m_options.escape().ifPresent(format::setQuoteEscape);
		m_parser = new CsvParser(settings);
		
		String header = m_options.header().get();
		RecordSchema outSchema = CsvUtils.buildRecordSchema(m_parser.parseLine(header));
		m_columns = outSchema.getColumns().toArray(new Column[0]);

		setInitialized(marmot, inputSchema, outSchema);
	}

	public void initialize(RecordSchema inputSchema) {
		Column col = inputSchema.findColumn(m_csvColName)
						.getOrThrow(() -> {
							String details = String.format("op=%s: input RecordSchema does not have "
															+ "'text' column", this);
							return new IllegalArgumentException(details);
						});
		Preconditions.checkArgument(col.type() == DataType.STRING,
									"invalid input RecordSchema");
		m_csvColIdx = col.ordinal();
		
		CsvParserSettings settings = new CsvParserSettings();
		m_options.nullValue().ifPresent(settings::setNullValue);
		
		CsvFormat format = settings.getFormat();
		format.setDelimiter(m_options.delimiter());
		m_options.quote().ifPresent(format::setQuote);
		m_options.escape().ifPresent(format::setQuoteEscape);
		m_parser = new CsvParser(settings);
		
		String header = m_options.header().get();
		RecordSchema outSchema = CsvUtils.buildRecordSchema(m_parser.parseLine(header));
		m_columns = outSchema.getColumns().toArray(new Column[0]);

		setInitialized(null, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		String text = input.getString(m_csvColIdx);
		try {
			String[] values = m_parser.parseLine(text);
			for ( int i =0; i < m_columns.length; ++i ) {
				if ( m_trimColumn ) {
					values[i] = values[i].trim();
				}
				if ( m_columns[i].type() != DataType.STRING ) {
					output.set(i, DataUtils.cast(values[i], m_columns[i].type()));
				}
				else {
					output.set(i, values[i]);
				}
			}
			
			return true;
		}
		catch ( Throwable e ) {
			e = Throwables.unwrapThrowable(e);
			if ( !m_options.throwParseError().map(f->f).getOrElse(false) ) {
				getLogger().warn("fails to parse csv: '" + text + "'", e);
				return false;
			}
			
			throw Throwables.toRuntimeException(e);
		}
	}

	public static ParseCsvText fromProto(ParseCsvProto proto) {
		ParseCsvOptions opts = ParseCsvOptions.fromProto(proto.getOptions());
		return new ParseCsvText(proto.getCsvColumn(), opts);
	}

	@Override
	public ParseCsvProto toProto() {
		return ParseCsvProto.newBuilder()
							.setCsvColumn(m_csvColName)
							.setOptions(m_options.toProto())
							.build();
	}
	
	@Override
	public String toString() {
		return String.format("%s[col=%s, schema=%s]", getClass().getSimpleName(), m_csvColName,
											m_options.header().get());
	}
}