package marmot.support;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import marmot.RecordSet;

/**
 * 
 * @author Kang-Woo Lee (ETRI)
 */
public interface RecordSetSerDe {
	public RecordSet read(InputStream is) throws IOException;
	public void write(RecordSet rset, OutputStream os) throws IOException;
}
