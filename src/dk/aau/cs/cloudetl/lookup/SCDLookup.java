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

import java.io.IOException;
import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.Utils;
import dk.aau.cs.cloudetl.hadoop.fs.FSUtil;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.io.SCDValuesWritable;
import dk.aau.cs.cloudetl.io.SequenceIndexFileReader;

public class SCDLookup extends Lookup implements Serializable {

	final protected String scdDate;

	public SCDLookup(DataWriter dimTable, String bkey, String scdAttr,
			int defaultValue) {
		super(dimTable, bkey, defaultValue);
		this.scdDate = scdAttr;
	}

	
	@Override
	public void setup(TaskAttemptContext context) { // Copy the map file from HDFS to local file system
		try {
			Configuration conf = context.getConfiguration();
			FileSystem localFS = FileSystem.getLocal(conf);
			Path[] idxPaths = FSUtil.getFilesOnly(new Path(conf.get(CEConstants.CLOUDETL_HOME), conf.get(CEConstants.CLOUDETL_LOOKUP_INDEX_DIR)), this.name+".*\\.idx", localFS);
			for (Path p : idxPaths){
				 FileStatus fileStatus = localFS.getFileStatus(p);
				 SequenceIndexFileReader<Text, SCDValuesWritable> reader = new SequenceIndexFileReader<Text, SCDValuesWritable>();
				 reader.initialize(new FileSplit(p, 0, fileStatus.getLen(), null), context);
				 while(reader.nextKeyValue()){
					 Text key = reader.getCurrentKey();
					 SCDValuesWritable value = reader.getCurrentValue();
					 index.put(key.toString(), value.copy());
				 }
				 
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	@Override
	public int lookup(RecordWritable record) throws IOException {
		try {
			Field bkeyField = record.getField(bkey);
			Object bkeyValue = bkeyField == null ? null : bkeyField.getValue();
			Field scdField = record.getField(scdDate);
			if (bkeyValue != null) {
				SCDValuesWritable val = (SCDValuesWritable) index.get(String.valueOf(bkeyValue));
				return val == null ? defaultValue : val.get(scdField.getValue(), defaultValue);
			} else {
				return defaultValue;
			}
		} catch (CEException e) {
			throw new IOException(e);
		}
	}
}
