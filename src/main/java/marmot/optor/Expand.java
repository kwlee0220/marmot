package marmot.optor;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import utils.Throwables;
import utils.Utilities;
import utils.func.FOption;

import marmot.Column;
import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordScript;
import marmot.RecordSetException;
import marmot.optor.support.ColumnVariableResolverFactory;
import marmot.optor.support.RecordLevelTransform;
import marmot.proto.optor.ExpandProto;
import marmot.support.DataUtils;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import marmot.type.DataType;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class Expand extends RecordLevelTransform implements PBSerializable<ExpandProto> {
	static final Logger s_logger = LoggerFactory.getLogger(Expand.class);

	private final String m_columnDecls;
	private boolean[] m_expandedMap;
	private FOption<RecordScript> m_script = FOption.empty();
	private FOption<RecordScriptExecution> m_scriptExec = FOption.empty();

	// MVEL 스크립트 처리시 레코드의 각 컬럼 값과 기타 임시 변수 값을 기록.
	private ColumnVariableResolverFactory m_vrFact;
	
	public Expand(String columnDecls) {
		Utilities.checkNotNullArgument(columnDecls, "added columns are null");
		
		m_columnDecls = columnDecls;
	}
	
	public Expand setInitializer(RecordScript initializer) {
		Utilities.checkNotNullArgument(initializer, "column initializer is null");
		
		m_script = FOption.ofNullable(initializer);
		m_scriptExec = m_script.map(RecordScriptExecution::of);
		
		return this;
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema expandedSchema = RecordSchema.parse(m_columnDecls);
		RecordSchema outSchema = expandedSchema.streamColumns()
												.fold(inputSchema.toBuilder(),
														(b,c) -> b.addOrReplaceColumnAll(c))
												.build();
		
		m_expandedMap = new boolean[outSchema.getColumnCount()];
		Arrays.fill(m_expandedMap, false); 
		expandedSchema.streamColumns()
					.forEach(col -> m_expandedMap[outSchema.getColumn(col.name()).ordinal()] = true);
		
		m_scriptExec.ifPresent(init -> {
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
		
		if ( m_scriptExec.isPresent() ) {
			m_vrFact.bind(output);
			
			try {
				m_scriptExec.get().execute(m_vrFact);
				
				getRecordSchema().streamColumns()
								.forEach(col -> {
									Object v = output.get(col.ordinal());
									output.set(col.ordinal(), DataUtils.cast(v, col.type()));
								});
			}
			catch ( Throwable e ) {
				e = Throwables.unwrapThrowable(e);
				Throwables.throwIfInstanceOf(e, RecordSetException.class);
				
				throw new RecordSetException("fails to run script on the record: " + input + " cause=" + e);
			}
		}
		else {
			for ( int i =0; i < m_expandedMap.length; ++i ) {
				if ( m_expandedMap[i] ) {
					Object v = output.get(i);
					DataType type = getRecordSchema().getColumnAt(i).type();
					try {
						output.set(i, DataUtils.cast(v, type));
					}
					catch ( Exception e ) {
						Column col = getInputRecordSchema().getColumnAt(i);
						String details = String.format("fails to conversion: src_col=%s, tar_type=%s, "
														+ "value=%s, cause=%s", col, type, v, e);
						throw new RecordSetException(details);
					}
				}
			}
		}
		
		return true;
	}
	
	@Override
	public String toString() {
		return String.format("expand[%s]", m_columnDecls);
	}

	public static Expand fromProto(ExpandProto proto) {
		Expand expand = new Expand(proto.getColumnDecls());
		switch ( proto.getOptionalColumnInitializerCase() ) {
			case COLUMN_INITIALIZER:
				RecordScript expr = RecordScript.fromProto(proto.getColumnInitializer());
				expand.setInitializer(expr);
				break;
			default:
		}
		
		return expand;
	}

	@Override
	public ExpandProto toProto() {
		ExpandProto.Builder builder = ExpandProto.newBuilder()
												.setColumnDecls(m_columnDecls);
		m_script.map(RecordScript::toProto).ifPresent(builder::setColumnInitializer);
		
		return builder.build();
	}
}
