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
import java.text.NumberFormat;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrepartitionReducer extends Reducer<Text, Text, Text, Text> {
	public static final Log LOG = LogFactory.getLog(PrepartitionReducer.class);
	private static final NumberFormat NUMBER_FORMAT = NumberFormat
			.getInstance();
	static {
		NUMBER_FORMAT.setMinimumIntegerDigits(5);
		NUMBER_FORMAT.setGroupingUsed(false);
	}
	int partNum;
	Configuration conf;
	String fileNamePatter;

	protected void setup(Context context) throws IOException,
			InterruptedException {
		this.conf = context.getConfiguration();
		String pattern = conf
				.get("cloudetl.prepart.colocated.filename.pattern");

		this.partNum = conf.getInt(PartConfigKeys.PARTITION_NUM, 1);
		this.fileNamePatter = conf.get(PartConfigKeys.OUTPUT_FILENAME) + "-%s";
	}

	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Text> itr = values.iterator();
		int partition = (Math.abs(key.toString().hashCode())) % this.partNum;
		String fileName = String.format(fileNamePatter,
				NUMBER_FORMAT.format(partition));
		Text outKey = new Text(fileName);
		while (itr.hasNext()) {
			context.write(outKey, itr.next());
		}
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
	}
}