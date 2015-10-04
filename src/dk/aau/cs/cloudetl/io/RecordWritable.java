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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.Writable;

import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.common.Utils;

public class RecordWritable implements Writable, Serializable, Configurable,
		Comparable<RecordWritable> {
	public enum State {
		ALIVE, NEW, MODIFIED, DELETED
	};
	private State state = State.ALIVE;
	
	private Configuration conf;
	private ArrayList<Field> fields;

	
	
	public RecordWritable() {
		this.fields = new ArrayList<Field>();
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		fields = new ArrayList<Field>(size);
		for (int i = 0; i < size; i++) {
			Field f = new Field();
			f.setConf(conf);
			f.readFields(in);
			fields.add(f);
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(fields.size());
		Iterator<Field> itr = fields.iterator();
		while (itr.hasNext()) {
			Field field = itr.next();
			field.write(out);
		}
	}
	
	public void addField(Field field) {
		fields.add(field);
	}

	public List<Field> getFields() {
		return fields;
	}

	public State getState() {
		return state;
	}

	public boolean isDeleted() {
		return state == State.DELETED;
	}

	void setModified() {
		if (state == State.MODIFIED || state == State.NEW) {
			return;
		}
		if (state == State.ALIVE) {
			state = State.MODIFIED;
		} else if (state == State.DELETED) {
			System.out
					.println("cannot modify record, it has already been marked as deleted");
		}
	}

	public void setAlive() {
		state = State.ALIVE;
	}

	public void delete() {
		this.state = State.DELETED;
	}

	public RecordWritable copyFrom(RecordWritable other) {
		this.fields.clear();
		List<Field> srcFields = other.getFields();
		for (int i=0; i<srcFields.size(); ++i){
			this.fields.add(new Field().copyFrom(srcFields.get(i)));
		}
		return this;
	}

	public void addAndCopyFrom(Field field){
		this.addField().copyFrom(field);
	}
	
	

	private Field getField0(int index) {
		if (index < 0 || index >= fields.size())
			return null;
		else
			return fields.get(index);
	}

	private Field getField0(String fieldName, boolean throwExceptionOnFailure) {
		return getField0(indexOf(fieldName, throwExceptionOnFailure));
	}

	public Field getField(int index) {
		return getField0(index);
	}

	public Field getField(int index, boolean createField) {
		if (createField) {
			while (index >= fields.size()) {
				addField();
			}
		}
		return getField0(index);
	}

	public Field getField(String fieldName) {
		return getField0(fieldName, true);
	}

	public Field getField(FieldType type){
		for (Field f : fields){
			if (f.getFieldType()==type){
				return f;
			}
		}
		return null;
	}
	
	public int indexOf(FieldType type){
		for (int i=0; i<fields.size(); ++i){
			Field f = fields.get(i);
			if (type==f.getFieldType()){
				return i;
			}
		}
		return -1;
	}
	
	public int indexOf(FieldType type, boolean create){
		for (int i=0; i<fields.size(); ++i){
			Field f = fields.get(i);
			if (type==f.getFieldType()){
				return i;
			}
		}
		fields.add(new Field(null, null, type));
		return fields.size()-1;
	}
	
	
	
	public Field getField(String fieldName, boolean createField) {
		if (!createField) {
			return getField0(indexOf(fieldName, true));
		} else {
			int index = indexOf(fieldName, false);
			if (index < 0) {
				return addField().setName(fieldName);
			}
			return getField0(index);
		}
	}

	public int getFieldCount() {
		return fields.size();
	}

	public Field addField() {
		Field f = new Field();
		fields.add(f);
		return f;
	}

	protected int indexOf(Field field) {
		int length = fields.size();
		for (int i = 0; i < length; i++) {
			if (fields.get(i) == field) {
				return i;
			}
		}
		return -1;
	}

	public int indexOf(String fieldName, boolean throwExceptionOnFailure) {
		int length = fields.size();
		for (int i = 0; i < length; i++) {
			if (Utils.namesMatch(getField(i).getName(), fieldName)) {
				return i;
			}
		}

		if (throwExceptionOnFailure) {
			System.out.println("unknown field [" + fieldName + "]");
		}
		return -1;
	}

	public boolean containsField(String fieldName) {
		return indexOf(fieldName, false) >= 0;
	}

	public Field removeField(int index) {
		Field removed = fields.remove(index);
		setModified();
		return removed;
	}

	public Field removeField(String columnName) {
		return removeField(indexOf(columnName, true));
	}

	public void moveField(int oldIndex, int newIndex) {
		Field field = fields.get(oldIndex);
		fields.remove(oldIndex);
		if (newIndex > oldIndex) {
			newIndex--;
		}
		fields.add(newIndex, field);
		setModified();
	}

	public void moveField(String columnName, int newIndex) {
		moveField(indexOf(columnName, true), newIndex);
	}




	public RecordWritable excludeFields(Set<String> fields) {
		for (int i = getFieldCount() - 1; i >= 0; i--) {
			if (fields.contains(getField(i).getName())) {
				removeField(i);
			}
		}
		return this;
	}

	public int size(){
		return fields.size();
	}
	
	public Text toText(){
		StringBuffer line = new StringBuffer();
		int size = fields.size();
		for (int i = 0; i < size; ++i) {
			Field f =  fields.get(i);
			Object value = f.getValue();
			line.append(value == null ? "" : value);
			if (i != size - 1) {
				line.append("\t");
			}
			f.setValue(null);
		}
		return new Text(line.toString());
	}
	
	public String toString() {
		StringBuffer strBuf = new StringBuffer();
		for (int i = 0; i < fields.size(); ++i) {
			strBuf.append(fields.get(i).getValue()).append("\t");
		}
		return strBuf.toString();
	}

	
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int compareTo(RecordWritable oOther) {

		// default -- they are equal
		return 0;
	}
}
