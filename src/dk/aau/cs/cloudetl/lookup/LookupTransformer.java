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

package dk.aau.cs.cloudetl.lookup;

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.hadoop.job.CETaskAttemptContextWrapper;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.transform.Transformer;

public class LookupTransformer extends Transformer implements Serializable {

	private Field field;
	private Lookup lookup;
	private boolean overwriteFields;

	public LookupTransformer(Field field, Lookup lookup, boolean overwriteFields) {
		this.field = field;
		this.lookup = lookup;
		this.overwriteFields = overwriteFields;
	}

	public LookupTransformer(Field field, Lookup lookup) {
		this(field, lookup, false);
	}

	public LookupTransformer(String fieldName, Lookup lookup) {
		this.field = new Field(fieldName, DataType.INT);
		this.lookup = lookup;
	}

	public Field getField() {
		return field;
	}

	public Lookup getLookup() {
		return lookup;
	}

	public boolean isOverwriteFields() {
		return overwriteFields;
	}

	public boolean transform(RecordWritable record) throws Throwable {
		int value = lookup.lookup(record);
		record.addField(new Field().copyFrom(field).setValue(value));
		return true;
	}

	public String toString() {
		return lookup + " using field(s) " + field;
	}

	@Override
	public void setup(TaskAttemptContext context) {
		CETaskAttemptContextWrapper<Map<String, Lookup>> ctxWrapper = (CETaskAttemptContextWrapper<Map<String, Lookup>>)context;
		Map<String, Lookup> lookups = ctxWrapper.get();
		String name = lookup.getName();
		if (lookups.containsKey(name)) {
			this.lookup = lookups.get(name);
		} else {
			this.lookup.setup(context);
			lookups.put(name, this.lookup);
		}
	}

	@Override
	public void cleanup(TaskAttemptContext context) {
		lookup.cleanup(context);
	}

}
