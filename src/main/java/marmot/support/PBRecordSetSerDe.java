package marmot.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import marmot.Record;
import marmot.RecordSchema;
import marmot.RecordSet;
import marmot.RecordSetException;
import marmot.proto.RecordProto;
import marmot.proto.RecordSchemaProto;
import marmot.protobuf.PBRecordProtos;
import marmot.rset.AbstractRecordSet;
import utils.io.IOUtils;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public class PBRecordSetSerDe implements RecordSetSerDe {
	@SuppressWarnings("deprecation")
	@Override
	public void write(RecordSet rset, OutputStream os) throws IOException {
		try {
			rset.getRecordSchema().toProto().writeDelimitedTo(os);
			
			Record rec = DefaultRecord.of(rset.getRecordSchema());
			while ( rset.next(rec) ) {
				PBRecordProtos.toProto(rec).writeDelimitedTo(os);
			}
		}
		finally {
			os.close();
			rset.closeQuietly();
		}
	}

	@Override
	public RecordSet read(InputStream is) throws IOException {
		return new PBStreamRecordSet(is);
	}
	
	private static class PBStreamRecordSet extends AbstractRecordSet {
		private final InputStream m_is;
		private final RecordSchema m_schema;
		
		PBStreamRecordSet(InputStream is) throws IOException {
			m_is = is;
			m_schema = RecordSchema.fromProto(RecordSchemaProto.parseDelimitedFrom(is));
		}
		
		@Override
		public RecordSchema getRecordSchema() {
			return m_schema;
		}

		@Override
		protected void closeInGuard() {
			IOUtils.closeQuietly(m_is);
		}
		
		@Override
		public boolean next(Record output) {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					PBRecordProtos.fromProto(proto, output);
					return true;
				}
				else {
					return false;
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException(e);
			}
		}
		
		@Override
		public Record nextCopy() {
			try {
				RecordProto proto = RecordProto.parseDelimitedFrom(m_is);
				if ( proto != null ) {
					return DefaultRecord.fromProto(m_schema, proto);
				}
				else {
					return null;
				}
			}
			catch ( IOException e ) {
				throw new RecordSetException(e);
			}
		}
	}
}
