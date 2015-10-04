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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.DataTypeUtil;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.common.Utils;
import dk.aau.cs.cloudetl.hadoop.fs.FSUtil;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.DimensionRecordsWriter;
import dk.aau.cs.cloudetl.io.LookIndexFileOutputFormat;
import dk.aau.cs.cloudetl.io.MapFileOutputFormat;
import dk.aau.cs.cloudetl.io.MultipleOutputs;
import dk.aau.cs.cloudetl.io.NamedFileOutputFormat;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.io.SCDValuesWritable;
import dk.aau.cs.cloudetl.io.SorrogateKeyWritable;
import dk.aau.cs.cloudetl.io.Transfer;

public class DimensionJobHandler implements JobHandler {
	private static final Log log = LogFactory.getLog(DimensionJobHandler.class);

	List<Transfer> transfers;
	Configuration conf;
	
	public DimensionJobHandler(List<Transfer> transfers, Configuration conf){
		this.transfers = transfers;
		this.conf = conf;
		
	}
	
	
	public static class DimensionTableMapper extends
			Mapper<LongWritable, Text, SorrogateKeyWritable, RecordWritable> {

		private List<DataReader> readers = new ArrayList<DataReader>();
		private List<DataWriter> writers = new ArrayList<DataWriter>();

		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String splitFilePath = fileSplit.getPath().toString();

				@SuppressWarnings("unchecked")
				List<Transfer> transfers = (List<Transfer>) Utils
						.deserializeBase64(conf.get(Tag.TRANSFERS));
				for (Transfer transfer : transfers) {
					DataReader reader = transfer.getReader();
					DataWriter writer = transfer.getWriter();
					if (splitFilePath.startsWith(reader.getInPath())) {
						reader.setup(context);
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

					SorrogateKeyWritable outKey = new SorrogateKeyWritable(writer.getName());
					RecordWritable outValue = new RecordWritable();

					List<Field> fields = destRecord.getFields();
					for (int j = 0; j < fields.size(); ++j) {
						Field destField = new Field().copyFrom(fields.get(j));
						int index = srcRecord.indexOf(destField.getName(),
								false);
						if (index != -1) {
							Field srcField = srcRecord.getField(index);
							if (destField.getFieldType() == FieldType.PRI) {
								outKey.setSID((Integer) DataTypeUtil
										.toJavaType(srcField.getValue(),
												destField.getDataType()));
							} else if (destField.getFieldType() == FieldType.LOOKUP) {
								outKey.setBKey(DataTypeUtil.toJavaType(
										srcField.getValue(),
										destField.getDataType()));
							} else {
								destField.setValue(DataTypeUtil.toJavaType(
										srcField.getValue(),
										destField.getDataType()));
								outValue.addField(destField);
							}
						} else {
							outValue.addField(destField);
						}
					}
					context.write(outKey, outValue);
				}
			} catch (CEException e) {
				e.printStackTrace();
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (int i = 0; i < readers.size(); ++i) {
				DataReader reader = readers.get(i);
				reader.cleanup(context);
			}
			readers.clear();
			writers.clear();
		}
	}

	public static class DimensionTableReducer
			extends
			Reducer<SorrogateKeyWritable, RecordWritable, Text, RecordWritable> {

		private Map<String, DimensionRecordsWriter> writers = new HashMap<String, DimensionRecordsWriter>();
		private Map<String, AtomicInteger> rowCounts = new HashMap<String, AtomicInteger>();
		

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			
			/*try {
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("/tmp/cloudetl1.conf"));
				
				conf.writeXml(bufferedWriter);
				System.out.println("Write config========================0");
				bufferedWriter.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}*/
			
			
			@SuppressWarnings("unchecked")
			List<Transfer> transfers = (List<Transfer>) Utils
					.deserializeBase64(conf.get(Tag.TRANSFERS));
			for (Transfer transfer : transfers) {
				DataWriter target = transfer.getWriter();
				String name = target.getName();
				writers.put(name, new DimensionRecordsWriter(context, target));
				rowCounts.put(name, new AtomicInteger(0));
			}
		}

		protected void reduce(SorrogateKeyWritable key,
				Iterable<RecordWritable> values, Context context)
				throws IOException, InterruptedException {
			try{
			String name = key.getName();
			
			DimensionRecordsWriter writer = writers.get(name);
			Iterator<RecordWritable> itr = values.iterator();
			if (writer.isSCDWriter()) {
				while (itr.hasNext()) {
					RecordWritable srcRecord = itr.next();
					RecordWritable destRecord = writer.newRecorderWritable();
					List<Field> destFields = destRecord.getFields();
					int index = 0;
					for (int i = 0; i < destFields.size(); ++i) {
						Field destField = destFields.get(i);
						if (destField.getFieldType() == FieldType.PRI) {
							destField.setValue(key.getSID());
						} else if (destField.getFieldType() == FieldType.LOOKUP) {
							destField.setValue(key.getBKey());
						} else {
							if (srcRecord != null) {
								Field srcField = srcRecord.getField(index++);
								if (srcField != null)
									destField.setValue(srcField.getValue());
							}
						}
					}
					writer.add(destRecord);
					rowCounts.get(name).incrementAndGet();
				}
				writer.writeAll();
			} else {
				int sid = key.getSID();
				Object bkey = key.getBKey();
				RecordWritable destRecord = writer.getRecorderWritable();
				List<Field> fields = destRecord.getFields();
				RecordWritable srcRecord = null;
				while (itr.hasNext()) {
					srcRecord = itr.next();
				}
				int index = 0;
				for (Field destField : fields) {
					FieldType fieldType = destField.getFieldType();
					if (fieldType == FieldType.PRI) {
						destField.setValue(sid);
					} else if (fieldType == FieldType.LOOKUP) {
						destField.setValue(bkey);
					} else {
						if (srcRecord != null) {
							Field srcField = srcRecord.getFields().get(index++);
							destField.setValue(srcField.getValue());
						}
					}
				}
				writer.write(destRecord);
				rowCounts.get(name).incrementAndGet();
			}
			}catch (CEException e){
				throw new IOException(e);
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			this.writers.clear();

			int size = rowCounts.size();
			/*KeyValueWritable[] kvs = new KeyValueWritable[size];
			int i = 0;
			for (Entry<String, AtomicInteger> entry : rowCounts.entrySet()) {
				kvs[i++] = new KeyValueWritable(new Text(entry.getKey()),
						new IntWritable(entry.getValue().intValue()));
			}

			ClientProtocol client = (ClientProtocol) RPC.waitForProxy(
					ClientProtocol.class, ClientProtocol.versionID,
					new InetSocketAddress(
							conf.get("cloudetl.meta.server.host"),
							CEConstants.SEQ_SERVER_PORT), conf);
			client.setTableRowNumber(kvs);
			RPC.stopProxy(client);*/
		}
	}

	@Override
	public boolean handle(List<Transfer> transfers, Configuration conf)
			throws CEException {
		boolean success = false;
		Path outputPath = null, tempInPathParent = null;
		
			
		try {
			// conf.set("mapred.job.tracker", "local"); // For debug.
			
			// FSUtil.addJarsToClassPath(conf);
			
			Set<String> outputDir = new HashSet<String>();
			Set<Path> inPaths = new HashSet<Path>();
			for (int i = 0; i < transfers.size(); ++i) {
				Transfer transfer = transfers.get(i);
				Path inPath = transfer.getInPath();
				FSUtil.ensurePath(inPath, conf);
				inPaths.add(inPath);
				outputDir.add(transfer.getOutputDir());
			}
			if (outputDir.size()!=1){
				throw new CEException("You can only set one output directory!");
			}
			outputPath = new Path(outputDir.toArray(new String[]{})[0]);
			
			
			if (FSUtil.exists(outputPath, conf)) {
				tempInPathParent = new Path(outputPath.getParent(), "temp");
				FSUtil.rename(outputPath, tempInPathParent, conf);
				int size = transfers.size();
				for (int i = 0; i < size; ++i) {
					Transfer transfer = transfers.get(i);
					DataReader reader = transfer.getWriter().asReader(conf,	tempInPathParent);
					DataWriter writer = transfer.getWriter();

					Path inPath = new Path(reader.getInPath());
					if (FSUtil.exists(inPath, conf)) {
						transfers.add(new Transfer(reader, writer));
						inPaths.add(inPath);
					}
				}
			}
			conf.set(Tag.TRANSFERS, Utils.serializeBase64(transfers));
			
			Job job = new Job(conf, CEConstants.DIMENSION_PROCESSING);
			
			for (Path inPath : inPaths) {
				FileInputFormat.addInputPath(job, inPath);
			}
			
			FSUtil.remove(outputPath, conf);
			FileOutputFormat.setOutputPath(job, outputPath);
			
			job.setNumReduceTasks(6*2); // Seems we have to set the reducer number explicitly, but it can also set in the command line by -D mapreduce.job.reduces=24		
			
			job.setInputFormatClass(TextInputFormat.class);
			job.setJarByClass(DimensionJobHandler.class);
			job.setMapperClass(DimensionTableMapper.class);

			job.setMapOutputKeyClass(SorrogateKeyWritable.class);
			job.setMapOutputValueClass(RecordWritable.class);

			job.setSortComparatorClass(CompositeKeyComparator.class);
			job.setPartitionerClass(BusinessKeyPartitioner.class);

			job.setGroupingComparatorClass(BusinessKeyGroupingComparator.class);
			job.setReducerClass(DimensionTableReducer.class);

			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(Text.class);
			
			MultipleOutputs.addMultiNamedOutput(job, CEConstants.NAMED_OUTPUT_TEXT, NamedFileOutputFormat.class, NullWritable.class, RecordWritable.class);
			MultipleOutputs.addMultiNamedOutput(job, CEConstants.NAMED_OUTPUT_MAP, LookIndexFileOutputFormat.class, Text.class, IntWritable.class);
			MultipleOutputs.addMultiNamedOutput(job, CEConstants.NAMED_OUTPUT_SCD_MAP, LookIndexFileOutputFormat.class, Text.class, SCDValuesWritable.class);
		
			job.waitForCompletion(true);
			
			if (!(success = job.isSuccessful())) {
				throw new CEException("Load failure!");
			}
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new CEException(e);
		} finally {
			try {
				if (success) {
					if (tempInPathParent!=null)
						FSUtil.remove(tempInPathParent, conf);
				} else {
					FSUtil.rename(tempInPathParent, outputPath, conf);
				}
				FSUtil.removeAllZeroByteFiles(outputPath.toString(), conf);
			} catch (IOException e) {
				throw new CEException(e);
			}
		}
		return success;
	}

	@Override
	public void run() {
		try {
			this.handle(transfers, conf);
		} catch (CEException e) {
			e.printStackTrace();
		}
	}

}
