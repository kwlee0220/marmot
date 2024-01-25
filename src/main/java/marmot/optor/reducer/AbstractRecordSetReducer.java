package marmot.optor.reducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import marmot.MarmotCore;
import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.mapreduce.MarmotMRContexts;
import marmot.optor.support.AbstractRecordSetFunction;
import marmot.support.DefaultRecord;
import utils.StopWatch;


/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public abstract class AbstractRecordSetReducer extends AbstractRecordSetFunction
												implements RecordSetReducer {
	private static final Logger s_logger = LoggerFactory.getLogger(AbstractRecordSetReducer.class);
	
	protected AbstractRecordSetReducer() {
		setLogger(s_logger);
	}
	
	/**
	 * 주어진 accumulator에 입력 레코드를 반영시킨다.
	 * 
	 * @param accum		Accumulator
	 * @param record	반영 시킬 입력 레코드
	 * @param index		반영 순차 번호
	 */
	protected void reduce(Record accum, Record record, int index) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void initialize(MarmotCore marmot, RecordSchema inputSchema) {
		setInitialized(marmot, inputSchema, inputSchema);
	}

	@Override
	public RecordSet apply(RecordSet input) {
		checkInitialized();
		
		return RecordSet.of(reduce(input));
	}

	/**
	 * 주어진 레코드세트를 reduce한 결과를 얻는다.
	 * 
	 * @param rset		reduce할 대상 레코드세트
	 * @return reduce된 결과 레코드.
	 */
	public Record reduce(RecordSet rset) {
		Record reduced = DefaultRecord.of(getRecordSchema());
		reduce(rset, reduced);
		return reduced;
	}

	public void reduce(RecordSet rset, Record reduced) {
		checkInitialized();
		
		reduced.clear();
		
		StopWatch elapsed = StopWatch.start();
		Record record = DefaultRecord.of(getInputRecordSchema());
		if ( rset.next(record) ) {
			reduce(reduced, record, 0);
			
			for ( int idx = 1; rset.next(record); ++idx ) {
				reduce(reduced, record, idx);
				
				if ( elapsed.getElapsedInSeconds() > 30 ) {
					MarmotMRContexts.reportProgress();
					
					getLogger().info("report: {}, count={}", toString(), idx);
					elapsed.restart();
				}
			}
		}
	}
}
