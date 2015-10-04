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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PrepartitionMapper extends
		Mapper<LongWritable, Text, Text, Text> {
	public static final Log LOG = LogFactory.getLog(PrepartitionMapper.class);
	String delim;
	Configuration conf;
	int keyIdx;

	protected void setup(Context context) {
		this.conf = context.getConfiguration();
		String pattern = conf.get("cloudetl.prepart.colocated.filename.pattern");

		
		this.delim = conf.get(PartConfigKeys.FIELD_DELIM) == null ? "\t" : conf
				.get(PartConfigKeys.FIELD_DELIM);
		this.keyIdx = conf.getInt(PartConfigKeys.KEY_INDEX, 0);

	}

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] fieldValues = line.split(this.delim);
		context.write(new Text(fieldValues[this.keyIdx]), value);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {

	}

}
