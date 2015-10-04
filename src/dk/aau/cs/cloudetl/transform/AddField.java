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
import java.sql.Time;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.lookup.Lookup;
import dk.aau.cs.cloudetl.lookup.SCDLookup;
import dk.aau.cs.cloudetl.metadata.SEQ;



public class AddField extends Transformer implements Serializable {

    private final String name;
    private final Object value;
    private final DataType fieldType;
    
	public AddField(String name, Object value, DataType fieldType) {
		this.name = name;
		this.value = value;
		this.fieldType = fieldType;
	}


	
	public AddField(String name, String value) {
		this(name, value, DataType.STRING);
	}

	public AddField(String name, Date value) {
		this(name, value, DataType.DATETIME);
	}

	public AddField(String name, java.sql.Date value) {
		this(name, value, DataType.DATE);
	}

	public AddField(String name, Time value) {
		this(name, value, DataType.TIME);
	}

	public AddField(String name, Integer value) {
		this(name, value, DataType.INT);
	}

	public AddField(String name, Long value) {
		this(name, value, DataType.LONG);
	}

	public AddField(String name, Short value) {
		this(name, value, DataType.SHORT);
	}

	public AddField(String name, Byte value) {
		this(name, value, DataType.BYTE);
	}

	public AddField(String name, Boolean value) {
		this(name, value, DataType.BOOLEAN);
	}

	public AddField(String name, Character value) {
		this(name, value, DataType.CHAR);
	}

	public AddField(String name, Double value) {
		this(name, value, DataType.DOUBLE);
	}

	public AddField(String name, Float value) {
		this(name, value, DataType.FLOAT);
	}

	public AddField(String name, byte[] value) {
		this(name, value, DataType.BLOB);
	}

	public AddField(String name, Object value) {
		this(name, value, DataType.UNDEFINED);
	}

	public String getName() {
        return name;
    }
    
    public Object getValue() {
        return value;
    }
    
    public DataType getFieldType() {
		return fieldType;
	}

    public boolean transform(RecordWritable record) throws Throwable {
        Field field = record.getField(name, true);
        if (value == null && fieldType != null) {
			field.setNull(fieldType);
		} else {
			if (value instanceof SEQ){
				SEQ seq = (SEQ) value;
				field.setValue(seq.nextSeq());
			} else {
				field.setValue(value);	
			}
		}

        return true;
    }
    
    public String toString() {
        return "set field \""+name+"\" to "+value + ":"+fieldType;
    }



	@Override
	public void cleanup(TaskAttemptContext context) {
		if (value instanceof SEQ){
			((SEQ) value).cleanup(context);
		}
		
	}

	@Override
	public void setup(TaskAttemptContext context) {
		if (value instanceof SEQ){
			((SEQ) value).setup(context);
		}
		
	}
}