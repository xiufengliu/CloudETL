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

package dk.aau.cs.cloudetl.hadoop.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.DataTypeUtil;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.Utils;
import dk.aau.cs.cloudetl.hadoop.fs.FSUtil;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.MultipleOutputs;
import dk.aau.cs.cloudetl.io.NamedFileOutputFormat;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.io.RecordsWriter;
import dk.aau.cs.cloudetl.io.SorrogateKeyWritable;
import dk.aau.cs.cloudetl.io.Transfer;
import dk.aau.cs.cloudetl.lookup.Lookup;

public class FactJobHandler implements JobHandler {

	private static final Log log = LogFactory.getLog(FactJobHandler.class);

	
	
	List<Transfer> transfers;
	Configuration conf;
	
	public FactJobHandler(List<Transfer> transfers, Configuration conf){
		this.transfers = transfers;
		this.conf = conf;
	}
	
	
	public static class FactTableMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private List<DataReader> readers = new ArrayList<DataReader>();
		private List<DataWriter> writers = new ArrayList<DataWriter>();
		private Map<String, Lookup> lookups = new HashMap<String, Lookup>();
		private Map<String, RecordsWriter> recWriters = new HashMap<String, RecordsWriter>();

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				
				CETaskAttemptContextWrapper<Map<String, Lookup>> ctxWrapper = new CETaskAttemptContextWrapper<Map<String, Lookup>>(context);
				ctxWrapper.set(lookups);
				
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String splitFilePath = fileSplit.getPath().toString();

				@SuppressWarnings("unchecked")
				List<Transfer> transfers = (List<Transfer>) Utils
						.deserializeBase64(conf.get(Tag.TRANSFERS));
				for (Transfer transfer : transfers) {
					DataReader reader = transfer.getReader();
					DataWriter writer = transfer.getWriter();
					String name = writer.getName();
					if (!recWriters.containsKey(name)){
						recWriters.put(name, new RecordsWriter(context, writer));
					}
					
					if (splitFilePath.startsWith(reader.getInPath())) {
						reader.setup(ctxWrapper);
						readers.add(reader);
						writers.add(writer);
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				for (int i = 0; i < readers.size(); ++i) {
					DataReader reader = readers.get(i);
					DataWriter writer = writers.get(i);

					RecordWritable srcRecord = reader.read(value.toString());
					RecordWritable destRecord = writer.getRecord();

					Text outKey = new Text(writer.getName());
					RecordWritable outValue = new RecordWritable();

					List<Field> fields = destRecord.getFields();
					for (int j = 0; j < fields.size(); ++j) {
						Field destField = new Field().copyFrom(fields.get(j));
						int index = srcRecord.indexOf(destField.getName(),	false);
						if (index != -1) {
							Field srcField = srcRecord.getField(index);
							destField.setValue(DataTypeUtil.toJavaType(
									srcField.getValue(),
									destField.getDataType()));
							outValue.addField(destField);
						} else {
							outValue.addField(destField);
						}
					}
					recWriters.get(writer.getName()).write(outValue);
				}
			} catch (CEException e) {
				e.printStackTrace();
			}

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			//readers.clear();
			//writers.clear();
			//lookups.clear();
		}
	}

	public static class FactTableReducer extends
			Reducer<SorrogateKeyWritable, RecordWritable, Text, Text> {
		protected void setup(Context context) throws IOException,
				InterruptedException {

		}

		protected void reduce(SorrogateKeyWritable key,
				Iterable<RecordWritable> values, Context context)
				throws IOException, InterruptedException {

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}
	}

	@Override
	public boolean handle(List<Transfer> transfers, Configuration conf)
			throws CEException {
		boolean success = false;
		Path outputPath = null;
		try {
			// conf.set("mapred.job.tracker", "local"); // For debug.
			conf.set(Tag.TRANSFERS, Utils.serializeBase64(transfers));
			// FSUtil.addJarsToClassPath(conf);
			
			
			Set<String> outputDir = new HashSet<String>();
			Set<Path> inFiles = new HashSet<Path>();
			for (int i = 0; i < transfers.size(); ++i) {
				Transfer transfer = transfers.get(i);
				Path inFile = transfer.getInPath();
				FSUtil.ensurePath(inFile, conf);
				inFiles.add(inFile);
				outputDir.add(transfer.getOutputDir());
			}
			if (outputDir.size()!=1){
				throw new CEException("You can only set one output directory!");
			}
			
			outputPath = new Path(outputDir.toArray(new String[]{})[0]);
			//,  Integer.toString((int) (100 * Math.random()))
			Job job = new Job(conf, CEConstants.FACT_PROCESSING);
			for (Path inFile : inFiles) {
				FileInputFormat.addInputPath(job, inFile);
			}
			
			FileOutputFormat.setOutputPath(job, outputPath);

			job.setInputFormatClass(TextInputFormat.class);
			job.setJarByClass(FactJobHandler.class);
			job.setMapperClass(FactTableMapper.class);

			//job.setMapOutputKeyClass(Text.class);
			//job.setMapOutputValueClass(Text.class);
			//job.setReducerClass(FactTableReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setNumReduceTasks(0);
			job.setOutputFormatClass(NamedFileOutputFormat.class);
			MultipleOutputs.addMultiNamedOutput(job, CEConstants.NAMED_OUTPUT_TEXT, NamedFileOutputFormat.class, NullWritable.class, RecordWritable.class);
			
			success = job.waitForCompletion(true);
			if (!success) {
				throw new CEException("Load failure!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new CEException(e);
		} finally {
			try {
				if (success) {
					//TODO: merges to the old facts.
				} else {
					FSUtil.remove(outputPath, conf);
				}
			} catch (IOException e) {
				throw new CEException(e);
			}
		}
		return success;
	}

	@Override
	public void run() {
		try {
			this.handle(this.transfers, this.conf);
		} catch (CEException e) {
			e.printStackTrace();
		}

	}


}
