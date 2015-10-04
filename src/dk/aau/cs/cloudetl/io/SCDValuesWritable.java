
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.Utils;

public class SCDValuesWritable implements Writable, Cloneable {

	List<SCDValueWritable> scdValues = new ArrayList<SCDValueWritable>();

	public void add(SCDValueWritable scdValue) {
		scdValues.add(scdValue);
	}

	public void clear(){
		scdValues.clear();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void write(DataOutput out) throws IOException {
		Collections.sort(scdValues);
		out.writeInt(scdValues.size());
		for (int i = scdValues.size()-1; i>=0 ; --i) {
			SCDValueWritable scdValue = scdValues.get(i);
			scdValue.write(out);
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		this.scdValues = new ArrayList<SCDValueWritable>();
		for (int i = 0; i < size; ++i) {
			SCDValueWritable scdValue = new SCDValueWritable();
			scdValue.readFields(in);
			this.scdValues.add(scdValue);
		}
		Collections.sort(scdValues);
	}

	public int get(Object scdDate, int defaultValue) throws CEException {
		long scdTime = scdDate==null?Utils.getCurrentTime():Utils.parseDateToLong(String.valueOf(scdDate));
		for (SCDValueWritable scdValue: scdValues){
			if (scdValue.getValidFrom()<=scdTime){
				return scdValue.get();
			}
		}
		return defaultValue;
	}
	
	public String toString(){
		StringBuffer bf = new StringBuffer();
		for (SCDValueWritable scdValue : scdValues){
			bf.append(scdValue.toString()).append(";");
		}
		return bf.toString();
	}
	
	

	 public SCDValuesWritable copy(){
		SCDValuesWritable copy = new SCDValuesWritable();
		for (SCDValueWritable scdValue : scdValues){
			copy.add(scdValue.copy());	
		}
		return copy;
	 }
}
