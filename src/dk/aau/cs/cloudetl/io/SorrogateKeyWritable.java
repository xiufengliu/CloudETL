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

import org.apache.hadoop.io.UTF8;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class SorrogateKeyWritable implements WritableComparable<SorrogateKeyWritable> {

	
	private Object bkey = "";
	private int sid = 0;
	private String name;

	public SorrogateKeyWritable(){
		this(null);
	}
	
	public SorrogateKeyWritable(String name){
		this.name = name;
	}
	

	public void set(Object bkey, int sid, String name) {
		this.bkey = bkey;
		this.sid = sid;
		this.name = name;
	}

	public void setBKey(Object bkey){
		this.bkey = bkey;
	}
	
	public void setSID(int sid){
		this.sid = sid;
	}
	
	public Object getBKey() {
		return this.bkey;
	}

	public int getSID() {
		return this.sid;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	

	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.bkey = UTF8.readString(in);
		this.sid = in.readInt();
		this.name = UTF8.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		 UTF8.writeString(out, String.valueOf(bkey));
		out.writeInt(this.sid);
		UTF8.writeString(out, name);
	}

	@Override
	public int compareTo(SorrogateKeyWritable other) {
		if (((String)bkey).compareTo((String)other.bkey) != 0) {
			return ((String)bkey).compareTo((String)other.bkey);
		} else if (this.sid != other.sid) {
			return sid < other.sid ? -1 : 1;
		} else {
			return 0;
		}

	}
	
	public String toString(){
		StringBuffer buf = new StringBuffer();
		buf.append(sid).append("\t").append(bkey);
		return buf.toString();
	}

	public static class DimensionCompositeKeyComparator extends WritableComparator {
		public DimensionCompositeKeyComparator() {
			super(SorrogateKeyWritable.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			return compareBytes(b1, s1, l1, b2, s2, l2);
		}
	}

	static { // register this comparator
		WritableComparator.define(SorrogateKeyWritable.class,	new DimensionCompositeKeyComparator());
	}
}