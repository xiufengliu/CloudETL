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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;

import dk.aau.cs.cloudetl.common.CEConfigurable;
import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.hadoop.fs.FSUtil;
import dk.aau.cs.cloudetl.hadoop.job.DimensionJobHandler;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.io.SCDValueWritable;
import dk.aau.cs.cloudetl.io.SCDValuesWritable;
import dk.aau.cs.cloudetl.io.SequenceIndexFileReader;
import dk.aau.cs.cloudetl.io.SlowlyChangingDimensionTableWriter;

public class Lookup implements CEConfigurable, Serializable {
	private static final Log log = LogFactory.getLog(Lookup.class);
	
	final protected String name;
	final protected String dimOutputDir;

	final protected String bkey;

	final protected int defaultValue;

	protected Map<String, Writable> index = new HashMap<String, Writable>();

	public Lookup(DataWriter writer, String bkey, int defaultValue) {
		this.bkey = bkey;
		this.defaultValue = defaultValue;
		this.dimOutputDir = writer.getOutputDir();
		this.name = writer.getName();
	}

	public int lookup(RecordWritable record) throws IOException {
		Field srcField = record.getField(this.bkey);
		Object srcValue = srcField == null ? null : srcField.getValue();
		if (srcValue != null) {
			IntWritable val = (IntWritable) index.get(String.valueOf(srcValue));
			return val==null?this.defaultValue:val.get();
		} else {
			return this.defaultValue;
		}
	}

	@Override
	public void setup(TaskAttemptContext context) { // Copy the map file from HDFS to local file system
		try {
			Configuration conf = context.getConfiguration();
			FileSystem localFS = FileSystem.getLocal(conf);
			Path[] idxPaths = FSUtil.getFilesOnly(new Path(conf.get(CEConstants.CLOUDETL_HOME), conf.get(CEConstants.CLOUDETL_LOOKUP_INDEX_DIR)), this.name+".*\\.idx", localFS);
			for (Path p : idxPaths){
				 FileStatus fileStatus = localFS.getFileStatus(p);
				 SequenceIndexFileReader<Text, IntWritable> reader = new SequenceIndexFileReader<Text, IntWritable>();
				 reader.initialize(new FileSplit(p, 0, fileStatus.getLen(), null), context);
				 while(reader.nextKeyValue()){
					 Text key = reader.getCurrentKey();
					 IntWritable value = reader.getCurrentValue();
					 index.put(key.toString(), new IntWritable(value.get()));
				 }
				 System.out.println(p);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(TaskAttemptContext context) {
		index.clear();
	}

	public String getName() {
		return this.name;
	}
}
