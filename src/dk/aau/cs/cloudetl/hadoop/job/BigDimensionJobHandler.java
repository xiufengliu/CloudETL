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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSpliCloudETLt;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

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
import dk.aau.cs.cloudetl.io.InsplitableTextInputFormat;
import dk.aau.cs.cloudetl.io.LookIndexFileOutputFormat;
import dk.aau.cs.cloudetl.io.MapFileOutputFormat;
import dk.aau.cs.cloudetl.io.MultipleOutputs;
import dk.aau.cs.cloudetl.io.NamedFileOutputFormat;
import dk.aau.cs.cloudetl.io.RecordWritable;
import dk.aau.cs.cloudetl.io.SCDValuesWritable;
import dk.aau.cs.cloudetl.io.SorrogateKeyWritable;
import dk.aau.cs.cloudetl.io.Transfer;

public class BigDimensionJobHandler implements JobHandler {
	private static final Log log = LogFactory.getLog(BigDimensionJobHandler.class);

	List<Transfer> transfers;
	Configuration conf;
	
	public BigDimensionJobHandler(List<Transfer> transfers, Configuration conf){
		this.transfers = transfers;
		this.conf = conf;
		
	}
	
	
	public static class DimensionTableMapper extends
			Mapper<LongWritable, Text, NullWritable, Text> {

		private DataReader incrReader = null;
		private DataWriter writer = null;
		private RecordReader<LongWritable, Text> dimReader = null;
		
		 @SuppressWarnings("unchecked")
		 private <T> T getSplitDetails(Path file, long offset, Configuration conf) 
		  throws IOException {
		   FileSystem fs = file.getFileSystem(conf);
		   FSDataInputStream inFile = fs.open(file);
		   inFile.seek(offset);
		   String className = Text.readString(inFile);
		   Class<T> cls;
		   try {
		     cls = (Class<T>) conf.getClassByName(className);
		   } catch (ClassNotFoundException ce) {
		     IOException wrap = new IOException("Split class " + className + 
		                                         " not found");
		     wrap.initCause(ce);
		     throw wrap;
		   }
		   SerializationFactory factory = new SerializationFactory(conf);
		   Deserializer<T> deserializer = 
		     (Deserializer<T>) factory.getDeserializer(cls);
		   deserializer.open(inFile);
		   T split = deserializer.deserialize(null);
		   long pos = inFile.getPos();
		   inFile.close();
		   return split;
		 }
		  
			
		protected void setup(Context context) {
			try {
				Configuration conf = context.getConfiguration();
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String splitFilePath = fileSplit.getPath().toString();

				@SuppressWarnings("unchecked")
				Transfer transfer = (Transfer) Utils.deserializeBase64(conf.get(Tag.TRANSFER));
				HashMap<String, String> colocatedFiles = (HashMap<String, String>)Utils.deserializeBase64(conf.get(Tag.COLOCATEDFILES));
				
				incrReader = transfer.getReader();
				incrReader.setup(context);
				
				writer = transfer.getWriter();
								
				String existingDimFilePath = colocatedFiles.get(splitFilePath);
				if (existingDimFilePath != null) {
					FileInputFormat<LongWritable, Text> inputFormat = (FileInputFormat<LongWritable, Text>) ReflectionUtils
							.newInstance(context.getInputFormatClass(), conf);
					Path existingDimPath = new Path(existingDimFilePath);
					FileSystem fs = existingDimPath.getFileSystem(conf);
					BlockLocation[] blkLocations = fs.getFileBlockLocations(
							existingDimPath, 0, Long.MAX_VALUE);
					InputSplit split = new FileSplit(existingDimPath, 0,
							Long.MAX_VALUE, blkLocations[0].getHosts()); 
					dimReader = inputFormat.createRecordReader(split, context);
					dimReader.initialize(split, context);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				try {
					RecordWritable incrRecord = makeIncrRecord(value.toString(), "\t");
					
					if (dimReader!=null){
						while (dimReader.nextKeyValue()){
							Text line = dimReader.getCurrentValue();
							RecordWritable dimRecord = this.makeDimRecord(line.toString(), "\t");
							Field d1 = dimRecord.getField(FieldType.LOOKUP);
							Field i1 = incrRecord.getField(FieldType.LOOKUP);
							int res = i1.getValueAsString().compareTo(d1.getValueAsString()); 
							if (res==0){
								Field d2 = dimRecord.getField(FieldType.SCD_VALIDFROM);
								Field d3 = dimRecord.getField(FieldType.SCD_VALIDTO);
								Field d4 = dimRecord.getField(FieldType.SCD_VERSION);
	
								
								Field i2 = incrRecord.getField(FieldType.SCD_VALIDFROM);
								Field i3 = incrRecord.getField(FieldType.SCD_VALIDTO);
								Field i4 = incrRecord.getField(FieldType.SCD_VERSION);
								
								if (Utils.dateAfter(i2.getValueAsString(), d2.getValueAsString())){
									d3.setValue(i2.getValue());
									d4.setValue((Integer)i4.getValue()+1);
								}
								context.write(NullWritable.get(), dimRecord.toText());
								context.write(NullWritable.get(), incrRecord.toText());
							} else if (res<0) {
								context.write(NullWritable.get(), incrRecord.toText());
								return;
							} else {
								context.write(NullWritable.get(), dimRecord.toText());
							}
						}
					} else {
						context.write(NullWritable.get(), incrRecord.toText());
					}
				} catch (CEException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
							
		}
		

		protected RecordWritable makeIncrRecord(String line, String delimiter) throws CEException{
			RecordWritable incrRawRecord = incrReader.read(line.toString());
			RecordWritable destRecord = writer.getRecord();
			RecordWritable incrRecord = new RecordWritable();
			
			List<Field> fields = destRecord.getFields();
			for (int i=0; i<fields.size(); ++i){
				Field destField = new Field().copyFrom(fields.get(i));
				Field srcField = incrRawRecord.getField(destField.getName(), false);
				if (srcField!=null){
					Object value = DataTypeUtil.toJavaType(srcField.getValue(), destField.getDataType());
					destField.setValue(value);
				}
				incrRecord.addField(destField);
			}
			return destRecord;
		}
		
		protected RecordWritable makeDimRecord(String line, String delimiter) throws CEException{
			RecordWritable destRecord = writer.getRecord();
			List<String> values = Utils.split(line, delimiter);
			List<Field> fields = destRecord.getFields();
			for (int i=0; i<fields.size(); ++i){
				Field field = fields.get(i);
				Object value = DataTypeUtil.toJavaType(values.get(i), field.getDataType());
				field.setValue(value);
			}
			return destRecord;
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			//reader.cleanup(context);
		}
	}

	public static class DimensionTableReducer
			extends
			Reducer<SorrogateKeyWritable, RecordWritable, Text, RecordWritable> {


		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			@SuppressWarnings("unchecked")
			List<Transfer> transfers = (List<Transfer>) Utils
					.deserializeBase64(conf.get(Tag.TRANSFERS));
			for (Transfer transfer : transfers) {
				DataWriter target = transfer.getWriter();
				String name = target.getName();
			}
		}

		protected void reduce(SorrogateKeyWritable key,	Iterable<RecordWritable> values, Context context)	throws IOException, InterruptedException {

		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
		}
	}

	@Override
	public boolean handle(List<Transfer> transfers, Configuration conf)
			throws CEException {
		boolean success = false;
		Path tempOutPath = null, outPath = null;
		try {
			Transfer transfer = transfers.get(0);
			Path inPath = transfer.getInPath();
			FSUtil.ensurePath(inPath, conf);
			
			tempOutPath = new Path(transfer.getOutputDir(), "temp");
			outPath = new Path(transfer.getOutputDir(), transfer.getName());
			FSUtil.remove(tempOutPath, conf);
			
			conf.set(Tag.TRANSFER, Utils.serializeBase64(transfer));
			
			// The temporary implementation for testing. It should be automatically read the regex from the configuration, and match the co-located files.
			HashMap<String, String> colocatedFiles = new HashMap<String, String>();
			colocatedFiles.put(new Path(inPath, "pages-00000").toString(), new Path(outPath, "pagedim-r-00000").toString());
			//colocatedFiles.put(new Path(inPath, "pages-00001").toString(), new Path(outPath, "pagedim-r-00001").toString());
			conf.set(Tag.COLOCATEDFILES, Utils.serializeBase64(colocatedFiles));
			
			Job job = new Job(conf, CEConstants.DIMENSION_PROCESSING);
			FileInputFormat.addInputPath(job, inPath);
			
			FileOutputFormat.setOutputPath(job, tempOutPath);
			job.setInputFormatClass(InsplitableTextInputFormat.class);
			job.setJarByClass(BigDimensionJobHandler.class);
			job.setMapperClass(DimensionTableMapper.class);

			//job.setReducerClass(IdentityReducer.class);
			
			job.setNumReduceTasks(0);   
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			
			
			//job.setOutputFormatClass(NamedFileOutputFormat.class);
			//MultipleOutputs.addMultiNamedOutput(job, CEConstants.NAMED_OUTPUT_TEXT, NamedFileOutputFormat.class, NullWritable.class, RecordWritable.class);
		
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
					//FSUtil.remove(outPath, conf);
					FSUtil.removeAllZeroByteFiles(tempOutPath.toString(), conf);
					//FSUtil.rename(tempOutPath, outPath, conf);
				}
			} catch (Exception e) {
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
