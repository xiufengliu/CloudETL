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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.BlockPlacementPolicy;
import org.apache.hadoop.hdfs.server.namenode.CEBlockPlacementPolicy;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Prepartitioner {

	public boolean run(Configuration conf, String[] args) throws Exception {
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);


		Job job = new Job(conf, "Pre-partition");
		
		job.setInputFormatClass(InSplitableTextInputFormat.class);
		
		//job.setInputFormatClass(LoalRawFileInputFormat.class);
		
		FileInputFormat.addInputPath(job, in);
		FileOutputFormat.setOutputPath(job, out);

		job.setNumReduceTasks(1);
		
		job.setMapperClass(PrepartitionMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setReducerClass(PrepartitionReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(FilenameByKeyMultipleTextOutputFormat.class);
		job.setJarByClass(Prepartitioner.class);
		
		return job.waitForCompletion(true);
	}

	public static void main(String[] args) {
		Options opts = buildOptions(new Options());
		try {
			Configuration conf = new Configuration();
			CommandLineParser parser = new GnuParser();
			CommandLine cmdLine = parser.parse(opts, args, true);

			if (cmdLine.getArgs().length < 2 || !processOptions(conf, cmdLine)) {
				throw new ParseException("");
			}
			
			
			Prepartitioner prePartitioner = new Prepartitioner();
			boolean res = prePartitioner.run(conf, cmdLine.getArgs());
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter
					.printHelp(
							"java "
									+ Prepartitioner.class.getSimpleName()
									+ " -D <delim> -F <output filename> -I <business key cloumn index> -N <num>  <src> <dst>",
							opts);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@SuppressWarnings("static-access")
	private static Options buildOptions(Options opts) {
		Option fn = OptionBuilder.withArgName("output filename").hasArg()
				.withDescription("specify the output filename").create("F");

		Option nparts = OptionBuilder.withArgName("num").hasArg()
				.withDescription("specify the number of the partitions")
				.create("N");

		Option delim = OptionBuilder.withArgName("delim").hasArg()
				.withDescription("specify the delimiter of the fields")
				.create("D");

		Option idx = OptionBuilder.withArgName("index").hasArg()
				.withDescription("specify the index of the partition key")
				.create("I");
		

		
		opts.addOption(fn);
		opts.addOption(nparts);
		opts.addOption(delim);
		opts.addOption(idx);

		return opts;
	}

	private static boolean processOptions(Configuration conf, CommandLine line) {
		if (line.hasOption("F")) {
			conf.set(PartConfigKeys.OUTPUT_FILENAME, line.getOptionValue("F"));
		} else {
			return false;
		}
		if (line.hasOption("N")) {
			conf.setInt(PartConfigKeys.PARTITION_NUM, Integer.parseInt(line.getOptionValue("N")));
		} else {
			return false;
		}
		if (line.hasOption("D")) {
			conf.set(PartConfigKeys.FIELD_DELIM, line.getOptionValue("D"));
		} else {
			return false;
		}
		if (line.hasOption("I")) {
			conf.setInt(PartConfigKeys.FIELD_DELIM, Integer.parseInt(line.getOptionValue("I")));
		} else {
			return false;
		}
		return true;
	}

}
