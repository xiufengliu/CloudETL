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
package dk.aau.cs.cloudetl.filter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.RecordWritable;

public class FilteringReader extends DataReader implements Serializable {

	private final List<FieldFilter> filters = new ArrayList<FieldFilter>();

	public FilteringReader(DataReader reader) {
		super(reader);
	}

	public FilteringReader add(FieldFilter... filterCollection) {
		for (FieldFilter f : filterCollection){
			this.filters.add(f);
		}
		return this;
	}

	@Override
	public void setup(TaskAttemptContext context){
		super.nestedReader.setup(context);
	}

	@Override
	protected RecordWritable readImpl(String line) throws Throwable {
		RecordWritable record;

		record = nestedReader.read(line);
		if (record == null) {
			return null;
		}
		return interceptRecord(record);
	}

	protected RecordWritable interceptRecord(RecordWritable record)
			throws Throwable {
		for (int i = 0; i < filters.size(); i++) {
			FieldFilter fieldFilter = filters.get(i);
			if (!fieldFilter.allow(record)) {
				return null;
			}
		}
		return record;
	}

	@Override
	public void cleanup(TaskAttemptContext context)  {
		super.nestedReader.cleanup(context);
	}


}