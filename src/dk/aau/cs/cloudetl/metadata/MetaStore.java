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

package dk.aau.cs.cloudetl.metadata;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.hadoop.fs.FSUtil;
import dk.aau.cs.cloudetl.io.KeyValueWritable;

public class MetaStore {
	Map<String, Integer> seqMap = new HashMap<String, Integer>();
	Path seqFile, rowsFile;
	Configuration conf;
	
	public MetaStore(Configuration conf){
			this.conf = conf;
			System.out.println(conf.get(CEConstants.CLOUDETL_HOME));
			System.out.println(conf.get(CEConstants.META_DIR));
			
			this.seqFile = new Path(new Path(conf.get(CEConstants.CLOUDETL_HOME), conf.get(CEConstants.META_DIR)), "cloudETL.seq");
			this.rowsFile = new Path(new Path(conf.get(CEConstants.CLOUDETL_HOME), conf.get(CEConstants.META_DIR)), "cloudETL.rows");

			this.loadIntoMemory();
		
	}
	
	synchronized public int getNextSeq(String seqName) {
		if (!seqMap.containsKey(seqName)) {
			seqMap.put(seqName, conf.getInt(CEConstants.SEQ_INCR_DELTA, 10000));
			return 0;
		} else {
			int ret = seqMap.get(seqName);
			seqMap.put(seqName, ret + conf.getInt(CEConstants.SEQ_INCR_DELTA, 10000));
			return ret;
		}
	}
	
	synchronized public void loadIntoMemory() {
		try {
			FileSystem fs = FileSystem.getLocal(conf);
			if (fs.exists(seqFile)) {
				SequenceFile.Reader reader = new SequenceFile.Reader(fs,
						seqFile, conf);
				Text key = new Text();
				IntWritable value = new IntWritable();
				while (reader.next(key, value)) {
					String name = key.toString();
					int seq = value.get();
					seqMap.put(name, seq);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	synchronized public void materialize() {
		try {
			FileSystem fs = FileSystem.getLocal(conf);
			Path tmp = new Path(seqFile.getParent() + Path.SEPARATOR
					+ "tmp.seq");
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
					tmp, Text.class, IntWritable.class);
			for (Entry<String, Integer> entry : seqMap.entrySet()) {
				String name = entry.getKey();
				int seq = entry.getValue();
				writer.append(new Text(name), new IntWritable(seq));
			}
			writer.close();

			FSUtil.replaceFile(new File(tmp.toString()),
					new File(seqFile.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	synchronized public void dimensionNumber(KeyValueWritable... rowNumbers) {
		try {
			FileSystem fs = FileSystem.getLocal(conf);
			Path tmp = new Path(rowsFile.getParent() + Path.SEPARATOR
					+ "tmp.rows");
			SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf,
					tmp, Text.class, IntWritable.class);

			writer.append(new Text("size"), new IntWritable(rowNumbers.length));

			for (KeyValueWritable rowNumber : rowNumbers) {
				writer.append(rowNumber.getKey(), rowNumber.getValue());
			}
			writer.close();

			FSUtil.replaceFile(new File(tmp.toString()),
					new File(rowsFile.toString()));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
