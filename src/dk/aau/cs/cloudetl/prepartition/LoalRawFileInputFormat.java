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
package dk.aau.cs.cloudetl.prepartition;

import java.io.IOException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

/**
 * InputFormat that generates a user-defined number of splits to inject data
 * into the database.
 */
public class LoalRawFileInputFormat extends
		CombineFileInputFormat<LongWritable, Object> {

	public static final Log LOG = LogFactory
			.getLog(LoalRawFileInputFormat.class.getName());

	public static final int DEFAULT_NUM_MAP_TASKS = 4;

	public LoalRawFileInputFormat() {
	}

	/**
	 * @return the number of bytes across all files in the job.
	 */
	private long getJobSize(JobContext job) throws IOException {
		List<FileStatus> stats = listStatus(job);
		long count = 0;
		for (FileStatus stat : stats) {
			count += stat.getLen();
		}

		return count;
	}

	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		// Set the max split size based on the number of map tasks we want.
		long numTasks = getNumMapTasks(job);
		long numFileBytes = getJobSize(job);
		long maxSplitSize = numFileBytes / numTasks;

		setMaxSplitSize(maxSplitSize);

		LOG.debug("Target numMapTasks=" + numTasks);
		LOG.debug("Total input bytes=" + numFileBytes);
		LOG.debug("maxSplitSize=" + maxSplitSize);

		List<InputSplit> splits = super.getSplits(job);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Generated splits:");
			for (InputSplit split : splits) {
				LOG.debug("  " + split);
			}
		}
		return splits;
	}

	@Override
	@SuppressWarnings("unchecked")
	public RecordReader createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {

		CombineFileSplit combineSplit = (CombineFileSplit) split;

		// Use CombineFileRecordReader since this can handle CombineFileSplits
		// and instantiate another RecordReader in a loop; do this with the
		// CombineShimRecordReader.
		RecordReader rr = new CombineFileRecordReader(combineSplit, context,
				CombineShimRecordReader.class);

		return rr;
	}

	/**
	 * Allows the user to control the number of map tasks used for this export
	 * job.
	 */
	public static void setNumMapTasks(JobContext job, int numTasks) {
		job.getConfiguration().setInt(PartConfigKeys.IMPORT_MAP_TASKS_NUM,
				numTasks);
	}

	/**
	 * @return the number of map tasks to use in this export job.
	 */
	public static int getNumMapTasks(JobContext job) {
		return job.getConfiguration().getInt(
				PartConfigKeys.IMPORT_MAP_TASKS_NUM, DEFAULT_NUM_MAP_TASKS);
	}

}