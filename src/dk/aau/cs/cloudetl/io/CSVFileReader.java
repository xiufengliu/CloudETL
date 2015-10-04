/*
 *
 * Copyright (c) 2011, Xiufeng Liu (xiliu@cs.aau.dk) and the eGovMon Consortium
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 *
 */
package dk.aau.cs.cloudetl.io;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.DataTypeUtil;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.Utils;

public class CSVFileReader extends DataReader implements Serializable {

	private static final long serialVersionUID = -476466677855257297L;

	String inPath;
	String delimiter;
	

	public CSVFileReader(String inPath) {
		this(inPath, "\t");
	}

	public CSVFileReader(String inPath, String delimiter) {
		super();
		this.inPath = inPath;
		this.delimiter = delimiter;
	}

	@Override
	public void setup(TaskAttemptContext context) {
	}

	public String getDelimiter() {
		return this.delimiter;
	}

	@Override
	public String getInPath() {
		return this.inPath;
	}

	@Override
	protected RecordWritable readImpl(String line) throws Throwable {
		RecordWritable record = new RecordWritable();
		List<String> values = Utils.split(line, delimiter);
		for (int i=0; i<fields.size(); ++i){
			Field field = new Field().copyFrom(fields.get(i));
			Object value = DataTypeUtil.toJavaType(values.get(i), field.getDataType());
			field.setValue(value);
			record.addField(field);
		}
		return record;
	}


	@Override
	public void cleanup(TaskAttemptContext context){
		// TODO Auto-generated method stub
		
	}


}
