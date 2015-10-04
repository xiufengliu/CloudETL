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
import java.util.ArrayList;
import java.util.List;

import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.common.CEConfigurable;

public abstract class DataReader implements CEConfigurable, Serializable {

	private static final long serialVersionUID = -6169327327711093885L;

	protected DataReader nestedReader;

	protected List<Field> fields;
	
	

	public DataReader() {
		this(null);
	}

	public DataReader(DataReader nestedReader) {
		this.nestedReader = nestedReader;
		this.fields = new ArrayList<Field>();
	}

	public DataReader getNestedReader() {
		return this.nestedReader;
	}

	public DataReader getRootReader() {
		if (getNestedReader() == null) {
			return this;
		} else {
			return getNestedReader().getRootReader();
		}
	}

	
	public DataReader setField(String fieldName, DataType dataType) {
		fields.add(new Field(fieldName, dataType, FieldType.OTHER));
		return this;
	}
	
	public DataReader setField(String fieldName, DataType dataType, FieldType fieldType) {
		fields.add(new Field(fieldName, dataType, fieldType));
		return this;
	}

	public DataReader setField(Field field) {
		fields.add(field);
		return this;
	}

	public DataReader setFields(List<Field> list) {
		for (Field field : list)
			this.fields.add(field);
		return this;
	}
	
	public List<Field> getFields() {
		return fields;
	}
	
	public RecordWritable read(String line) throws CEException {
		try {
			RecordWritable record = readImpl(line);
			return record;
		} catch (Throwable e) {
			throw new CEException(e);
		}
	}
	
	public String getInPath(){
		return this.nestedReader.getInPath();
	}

	protected abstract RecordWritable readImpl(String line) throws Throwable;

}
