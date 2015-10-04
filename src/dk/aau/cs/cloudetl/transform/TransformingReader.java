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
package dk.aau.cs.cloudetl.transform;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.RecordWritable;

public class TransformingReader extends DataReader implements Serializable {

	private final List<Transformer> transformers = new ArrayList<Transformer>();
	private Transformer currentTransformer;
	private int currentTransformerIndex;

	public TransformingReader(DataReader reader) {
		super(reader);
	}

	public TransformingReader add(Transformer... transformer) {
		for (int i = 0; i < transformer.length; i++) {
			transformers.add(transformer[i]);
			transformer[i].setReader(this);
		}
		return this;
	}

	public int getCount() {
		return transformers.size();
	}

	public Transformer get(int index) {
		return transformers.get(index);
	}

	protected RecordWritable interceptRecord(RecordWritable record)
			throws Throwable {
		for (currentTransformerIndex = 0; currentTransformerIndex < getCount(); currentTransformerIndex++) {
			currentTransformer = get(currentTransformerIndex);
			if (!transformRecord(record, currentTransformer)) {
				return null;
			}
		}
		return record;
	}

	protected boolean transformRecord(RecordWritable record,
			Transformer transformer) {
		boolean transformed = false;
		try {
			transformed = transformer.transform(record);
		} catch (Exception e) {
			e.printStackTrace();
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return transformed;
	}

	protected RecordWritable readImpl(String line) throws Throwable {
		RecordWritable record;

		record = nestedReader.read(line);
		if (record == null) {
			return null;
		}
		record = interceptRecord(record);
		return record;
	}

	@Override
	public void cleanup(TaskAttemptContext context) {
		super.nestedReader.cleanup(context);
		for (int i = 0; i < transformers.size(); ++i) {
			Transformer transformer = transformers.get(i);
			transformer.cleanup(context);
		}

	}

	@Override
	public void setup(TaskAttemptContext context) {
		super.nestedReader.setup(context);
		for (int i = 0; i < transformers.size(); ++i) {
			Transformer transformer = transformers.get(i);
			transformer.setup(context);
		}

	}
}
