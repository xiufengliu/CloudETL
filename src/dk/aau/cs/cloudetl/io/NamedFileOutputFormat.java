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

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import dk.aau.cs.cloudetl.hadoop.job.DimensionJobHandler;

/** An {@link OutputFormat} that writes plain text files. */
public class NamedFileOutputFormat<K, V extends RecordWritable> extends
		TextOutputFormat {
	private static final Log log = LogFactory.getLog(NamedFileOutputFormat.class);

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		String keyValueSeparator = conf.get(
				"mapred.textoutputformat.separator", "\t");
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		
		//Path file = getDefaultWorkFile(job, extension);
		//String name = conf.get("mo.namedOutputs.multi", "unknowntable");
		Path file =  this.getDefaultWorkFile(job, extension); //new Path(new Path(getOutputPath(job), name), getUniqueFile(job, name, extension));
		
		FileSystem fs = file.getFileSystem(conf);
		if (!isCompressed) {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(fileOut, keyValueSeparator);
		} else {
			FSDataOutputStream fileOut = fs.create(file, false);
			return new LineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileOut)), keyValueSeparator);
		}
	}
		@Override
	  public Path getDefaultWorkFile(TaskAttemptContext context,
            String extension) throws IOException{
			Configuration conf = context.getConfiguration();
			String name = conf.get("mo.namedOutputs.multi", "unknowntable");
			return new Path(new Path(getOutputPath(context), name), getUniqueFile(context, name, extension));
		}
}
