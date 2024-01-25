package marmot.optor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.DefineColumnProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class DefineColumn extends RecordLevelTransform
							implements PBSerializable<DefineColumnProto> {
	static final Logger s_logger = LoggerFactory.getLogger(DefineColumn.class);

	private final String m_colDecl;
	private final FOption<RecordScript> m_columnInitScript;
	private final FOption<RecordScriptExecution> m_columnInitExec;
	
	private Column m_definedCol;	// set during initialization
	private int m_ordinal = -1;

	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private ColumnVariableResolverFactory m_vrFact;
	
	public DefineColumn(String colDecl, RecordScript initalizer) {
		Utilities.checkNotNullArgument(colDecl, "column declaration");
		Utilities.checkNotNullArgument(initalizer, "column initializer");
		
		m_colDecl = colDecl;
		m_columnInitScript = FOption.of(initalizer);
		m_columnInitExec = m_columnInitScript.map(RecordScriptExecution::of);
	}
	
	public DefineColumn(String colDecl) {
		Utilities.checkNotNullArgument(colDecl, "column declaration");
		
		m_colDecl = colDecl;
		m_columnInitScript = FOption.empty();
		m_columnInitExec = FOption.empty();
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema schema = RecordSchema.parse(m_colDecl);
		if ( schema.getColumnCount() > 1 ) {
			throw new IllegalArgumentException("too many columns are defined: decl=" + m_colDecl);
		}
		else if ( schema.getColumnCount() == 0 ) {
			throw new IllegalArgumentException("no column is defined");
		}
		m_definedCol = schema.getColumnAt(0);
		
		RecordSchema outSchema = inputSchema.toBuilder()
											.addOrReplaceColumn(m_definedCol.name(), m_definedCol.type())
											.build();
		
		m_ordinal = outSchema.getColumn(m_definedCol.name()).ordinal();
		m_columnInitExec.ifPresent(init -> {
			m_vrFact = new ColumnVariableResolverFactory(outSchema, init.getArgumentAll());
			init.initialize(m_vrFact);
			
			getLogger().debug("initialized={}", this);
		});
		
		setInitialized(marmot, inputSchema, outSchema);
	}

	@Override
	public boolean transform(Record input, Record output) {
		checkInitialized();
		
		output.setAll(input.getAll());
		
		if ( m_columnInitExec.isPresent() ) {
			m_vrFact.bind(output);
			
			try {
				Object initValue = m_columnInitExec.get().execute(m_vrFact);
				output.set(m_ordinal, DataUtils.cast(initValue, m_definedCol.type()));
				
				return true;
			}
			catch ( Throwable e ) {
				e = Throwables.unwrapThrowable(e);
				Throwables.throwIfInstanceOf(e, RecordSetException.class);
				
				throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
			}
		}
		else if ( m_ordinal < getInputRecordSchema().getColumnCount() ) {
			// 기존 동일 컬럼의 타입이 변경되는 경우.
			Object src = input.get(m_ordinal);
			output.set(m_ordinal, DataUtils.cast(src, m_definedCol.type()));
		}
		else {
			output.set(m_ordinal, null);
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		String initStr = m_columnInitScript.map(init -> String.format(" = %s", init))
											.getOrElse("");
		return String.format("define_column[%s]%s]", m_colDecl, initStr);
	}

	public static DefineColumn fromProto(DefineColumnProto proto) {
		DefineColumn defineColumn = null;
		switch ( proto.getOptionalColumnInitializerCase() ) {
			case COLUMN_INITIALIZER:
				RecordScript initScript = RecordScript.fromProto(proto.getColumnInitializer());
				defineColumn = new DefineColumn(proto.getColumnDecl(), initScript);
				break;
			case OPTIONALCOLUMNINITIALIZER_NOT_SET:
				defineColumn = new DefineColumn(proto.getColumnDecl());
				break;
			default:
				throw new AssertionError();
		}
		
		return defineColumn;
	}

	@Override
	public DefineColumnProto toProto() {
		DefineColumnProto.Builder builder = DefineColumnProto.newBuilder()
															.setColumnDecl(m_colDecl);
		m_columnInitScript.map(RecordScript::toProto).ifPresent(builder::setColumnInitializer);
		
		return builder.build();
	}
}
