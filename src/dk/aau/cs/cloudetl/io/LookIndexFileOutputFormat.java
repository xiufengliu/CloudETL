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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;

import dk.aau.cs.cloudetl.common.Utils;

/** An {@link OutputFormat} that writes {@link SequenceFile}s. */
@InterfaceAudience.Public
@InterfaceStability.Stable
public class LookIndexFileOutputFormat<K, V> extends FileOutputFormat<K, V> {

	protected SequenceFile.Writer getSequenceWriter(TaskAttemptContext context,
			Class<?> keyClass, Class<?> valueClass) throws IOException {
		Configuration conf = context.getConfiguration();

		CompressionCodec codec = null;
		CompressionType compressionType = CompressionType.NONE;
		if (getCompressOutput(context)) {
			// find the kind of compression to do
			compressionType = getOutputCompressionType(context);
			// find the right codec
			Class<?> codecClass = getOutputCompressorClass(context,
					DefaultCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
		}
		// get the path of the temporary output file
		Path file = getDefaultWorkFile(context, ".idx");
		FileSystem fs = file.getFileSystem(conf);
		return SequenceFile.createWriter(fs, conf, file, keyClass, valueClass,
				compressionType, codec, context);
	}

	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		final SequenceFile.Writer out = getSequenceWriter(context,
				context.getOutputKeyClass(), context.getOutputValueClass());

		return new RecordWriter<K, V>() {

			public void write(K key, V value) throws IOException {
				out.append(key, value);
			}

			public void close(TaskAttemptContext context) throws IOException {
				out.close();
			}
		};
	}

	/**
	 * Get the {@link CompressionType} for the output {@link SequenceFile}.
	 * 
	 * @param job
	 *            the {@link Job}
	 * @return the {@link CompressionType} for the output {@link SequenceFile},
	 *         defaulting to {@link CompressionType#RECORD}
	 */
	public static CompressionType getOutputCompressionType(JobContext job) {
		String val = job.getConfiguration().get(FileOutputFormat.COMPRESS_TYPE,
				CompressionType.RECORD.toString());
		return CompressionType.valueOf(val);
	}

	/**
	 * Set the {@link CompressionType} for the output {@link SequenceFile}.
	 * 
	 * @param job
	 *            the {@link Job} to modify
	 * @param style
	 *            the {@link CompressionType} for the output
	 *            {@link SequenceFile}
	 */
	public static void setOutputCompressionType(Job job, CompressionType style) {
		setCompressOutput(job, true);
		job.getConfiguration().set(FileOutputFormat.COMPRESS_TYPE,
				style.toString());
	}

	@Override
	public Path getDefaultWorkFile(TaskAttemptContext context, String extension)
			throws IOException {
		Configuration conf = context.getConfiguration();
		String name = conf.get("mo.namedOutputs.multi", "unknowntable"); // table name
		TaskID taskId = context.getTaskAttemptID().getTaskID();
		int partition = taskId.getId();
		StringBuilder indexFileName = new StringBuilder(name).append("_")
				.append(Utils.getCurrentTimeStamp()).append("_").append(partition).append(extension);
		return new Path(getOutputPath(context), indexFileName.toString());
	}
	


}