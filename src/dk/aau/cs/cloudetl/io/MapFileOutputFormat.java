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

import java.io.IOException;

import javax.xml.soap.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileOutputFormat<K extends WritableComparable, V extends Writable>
		extends FileOutputFormat<K, V> {

	protected static class RecordLineWriter<K extends WritableComparable, V extends Writable>
			extends RecordWriter<K, V> {
		Writer out;

		public RecordLineWriter(Writer out) {
			this.out = out;
		}

		@Override
		public void write(K key, V value) throws IOException,
				InterruptedException {
			out.append(key, value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException {
			this.out.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		// get the path of the temporary output file
		Configuration conf = job.getConfiguration();
		Path file = getDefaultWorkFile(job, "");

		FileSystem fs = file.getFileSystem(conf);
		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		// if (getCompressOutput(job)) {
		// find the kind of compression to do
		String val = conf.get("mapred.output.compression.type",
				CompressionType.RECORD.toString());
		compressionType = CompressionType.valueOf(val);
		// find the right codec
		Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
				job, DefaultCodec.class); // BZip2Codec
		codec = ReflectionUtils.newInstance(DefaultCodec.class, conf);
		// }

		final Writer out = SequenceFile.createWriter(fs, conf, file, job
				.getOutputKeyClass().asSubclass(WritableComparable.class), job
				.getOutputValueClass().asSubclass(Writable.class),
				compressionType, codec, null);
		return new RecordLineWriter(out);
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		Configuration conf = context.getConfiguration();
		FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
		String name = conf.get("mo.namedOutputs.multi", "unknowntable"); // table name
		return new Path(new Path(getOutputPath(context), name + "_map"),
				getUniqueFile(context, "part", extension));
	}
}
