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
package dk.aau.cs.cloudetl.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.Collator;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;




public final class Field implements Configurable, Writable, Comparable<Field>, Serializable {
    
    private String name;
    private DataType dataType;
    private Object value;
    private FieldType fieldType;

    private Configuration conf;
  
    public Field(){
    	this(null, DataType.STRING, FieldType.OTHER);
    }
    
    public  Field(String name, DataType dataType){
    	this(name, dataType, null, FieldType.OTHER);
    }
    
    public  Field(String name, DataType dataType, FieldType fieldType){
    	this(name, dataType, null, fieldType);
    }
    
    public  Field(String name, DataType type, Object value, FieldType fieldType){
    	this.name = name;
    	this.dataType = type;
    	this.value = value;
    	this.fieldType = fieldType;
    	this.conf = null;
    }
    
    public Field copyFrom(Field field) {
        this.name = field.name;
        this.dataType = field.dataType;
        this.value = field.value;
        this.fieldType = field.fieldType;
        return this;
    }
    
    public Field copyFrom(Field field, boolean copyName) {
        if(copyName) {
            this.name = field.name;
        }
        this.dataType = field.dataType;
        this.value = field.value;
        this.fieldType = field.fieldType;
        return this;
    }
    
 
    private Field setValue(DataType type, Object value) {
        this.dataType = type;
        this.value = value;
        return this;
    }
    
    public Field setName(String name) {
        this.name = name;
        return this;
    }

    public String getName(){
    	return this.name;
    }
    
    public DataType getDataType() {
        return dataType;
    }
    
    public FieldType getFieldType() {
        return fieldType;
    }

    public boolean isNull() {
        return value == null;
    }

    public String getValueAsString() {
        Object value = getValue();
        return value == null?"":value.toString();
    }

    public Field setValue(Object value) {
        this.value = value;
        return this;
    }

    public Field setNull(DataType type) {
        return setValue(type, null);
    }

    
    public Object getValue() {
        return value;
    }

    public int compareTo(Field o) {
        return compareTo(o, null);
    }
    
    public int compareTo(Field o, Collator collator) {
        if (o == null) {
            return 1;
        }
        
        if (this == o) {
            return 0;
        }
        
        Field field = (Field)o;
        return Utils.compare(this.value, field.value, collator);
    }
    
    public boolean equals(Object o) {
        return equals(o, null);
    }
    
    
    public boolean equals(Object o, Collator collator) {
        return compareTo((Field)o, collator) == 0;
    }
    
    public boolean isNotNull() {
        return value != null;
    }

 
    public String toString() {
      String s = "["+name+"]:" + getDataType() + "=[" + value + "]";
      if (value != null) {
          s += ':' + Utils.getTypeName(value);
      }
      return s;
  }



	@Override
	public void readFields(DataInput in) throws IOException {
		value =ObjectWritable.readObject(in, conf);
	}



	@Override
	public void write(DataOutput out) throws IOException {
		ObjectWritable.writeObject(out, value, DataTypeUtil.getJavaClass(dataType), conf);
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

}