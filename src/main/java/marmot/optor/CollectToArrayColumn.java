package marmot.optor;

import java.lang.reflect.Array;

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
import marmot.proto.optor.CollectToArrayColumnProto;
import marmot.support.DefaultRecord;
import marmot.support.PBSerializable;
import marmot.support.RecordScriptExecution;
import marmot.type.ContainerDataType;
import marmot.type.DataType;
import utils.Throwables;
import utils.Utilities;
import utils.stream.BooleanFStream;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class CollectToArrayColumn extends RecordLevelTransform
									implements PBSerializable<CollectToArrayColumnProto> {
	static final Logger s_logger = LoggerFactory.getLogger(CollectToArrayColumn.class);

	private final String m_colDecl;
	private final RecordScript m_script;
	private final RecordScriptExecution m_selectExpr;

	private Column m_definedCol;	// set during initialization
	private int m_arrayLength;
	private boolean[] m_mapping;
	
	public CollectToArrayColumn(String colDecl, String selectExpr) {
		this(colDecl, RecordScript.of(selectExpr));
	}
	
	private CollectToArrayColumn(String colDecl, RecordScript selectExpr) {
		Utilities.checkNotNullArgument(selectExpr, "update script is null");
		
		m_colDecl = colDecl;
		m_script = selectExpr;
		m_selectExpr = RecordScriptExecution.of(selectExpr);
		setLogger(s_logger);
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		RecordSchema schema = RecordSchema.builder()
											.addColumn("column", DataType.STRING)
											.build();
		Record colRec = DefaultRecord.of(schema);
		ColumnVariableResolverFactory vrFact = new ColumnVariableResolverFactory(schema,
																		m_script.getArgumentAll());
		m_selectExpr.initialize(vrFact);
		
		m_mapping = inputSchema.streamColumns()
								.mapToBoolean(col -> {
									colRec.set(0, col.name());
									vrFact.bind(colRec);
									return (Boolean)m_selectExpr.execute(vrFact);
								})
								.toArray();
		m_arrayLength = (int)BooleanFStream.of(m_mapping).filter(v -> v).count();
		
		RecordSchema colDeclSchema = RecordSchema.parse(m_colDecl);
		if ( colDeclSchema.getColumnCount() > 1 ) {
			throw new IllegalArgumentException("too many columns are defined: decl=" + m_colDecl);
		}
		else if ( colDeclSchema.getColumnCount() == 0 ) {
			throw new IllegalArgumentException("no column is defined");
		}
		m_definedCol = colDeclSchema.getColumnAt(0);
		
		RecordSchema outSchema = inputSchema.streamColumns()
											.zipWithIndex()
											.filter(t -> !m_mapping[t.index()])
											.fold(RecordSchema.builder(), (b,c) -> b.addColumn(c.value()))
											.addColumn(m_definedCol)
											.build();
		setInitialized(marmot, inputSchema, outSchema);
	}
	
	@Override
	public boolean transform(Record input, Record output) {
		try {
			int colIdx = 0;
			int elmIdx = 0;
			DataType ctype = ((ContainerDataType)m_definedCol.type()).getContainerType();
			Object array = Array.newInstance(ctype.getInstanceClass(), m_arrayLength);
			for ( int i =0; i < input.getColumnCount(); ++i ) {
				Object v = input.get(i);
				if ( m_mapping[i] ) {
					Array.set(array, elmIdx++, v);
				}
				else {
					output.set(colIdx++, v);
				}
			}
			output.set(output.getColumnCount()-1, array);
			
			return true;
		}
		catch ( Throwable e ) {
			e = Throwables.unwrapThrowable(e);
			Throwables.throwIfInstanceOf(e, RecordSetException.class);
			
			throw new RecordSetException("fails to run script on the record: " + input + ", cause=" + e);
		}
	}
	
	@Override
	public String toString() {
		String expr = m_script.getScript();
		if ( expr.length() > 50 ) {
			expr = expr.substring(0, 50) + "...";
		}
		
		return String.format("%s: to=%s, expr='%s']", getClass().getSimpleName(), m_colDecl, expr);
	}

	public static CollectToArrayColumn fromProto(CollectToArrayColumnProto proto) {
		RecordScript expr = RecordScript.fromProto(proto.getSelector());
		return new CollectToArrayColumn(proto.getColumnDecl(), expr);
	}

	@Override
	public CollectToArrayColumnProto toProto() {
		return CollectToArrayColumnProto.newBuilder()
										.setColumnDecl(m_colDecl)
										.setSelector(m_script.toProto())
										.build();
	}
}
