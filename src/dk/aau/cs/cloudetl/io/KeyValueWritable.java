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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * A WritableComparable containing a key-value WritableComparable pair.
 * 
 * @param <K>
 *            the class of key
 * @param <V>
 *            the class of value
 */
public class KeyValueWritable
		implements Writable {

	protected Text key = null;
	protected IntWritable value = null;

	public KeyValueWritable() {
	}

	public KeyValueWritable(Text key, IntWritable value) {
		setKey(key);
		setValue(value);
	}

	public Text getKey() {
		return key;
	}

	public void setKey(Text key) {
		this.key = key;
	}

	public IntWritable getValue() {
		return value;
	}

	public void setValue(IntWritable value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
			if (key==null){
				key = new Text();
				value = new IntWritable();
			}
			key.readFields(in);
			value.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KeyValueWritable other = (KeyValueWritable) obj;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	

	/*@Override
	public int compareTo(KeyValueWritable<K, V> o) {
		int cmp = key.compareTo(o.key);
		if (cmp != 0)
			return cmp;

		return value.compareTo(o.value);
	}*/
}