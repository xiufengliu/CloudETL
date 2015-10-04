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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.FactTableWriter;
import dk.aau.cs.cloudetl.io.Transfer;
import dk.aau.cs.cloudetl.lookup.Lookup;
import dk.aau.cs.cloudetl.metadata.MetaServerRPC;
import dk.aau.cs.cloudetl.metadata.MetaServer;

public class JobPlanner {
	private static final Log log = LogFactory.getLog(JobPlanner.class);
	Configuration conf;
	List<Transfer> dimTransfers = new ArrayList<Transfer>();
	List<Transfer> factTransfers = new ArrayList<Transfer>();

	public JobPlanner() {
		conf = new Configuration();
		conf.addResource(new Path(CEConstants.CLOUDETL_CONFIG));
	}

	public JobPlanner addTransfer(DataReader reader, DataWriter writer)
			throws CEException {
		if (writer instanceof FactTableWriter){
			factTransfers.add(new Transfer(reader, writer));
		} else {
			dimTransfers.add(new Transfer(reader, writer));
		}
		return this;
	}

	public void start() {
		try {
			//conf.addResource(new Path("/data1/hadoop-0.21.0/conf/core-site.xml"));
			//conf.addResource(new Path("/data1/hadoop-0.21.0/conf/hdfs-site.xml"));
			
			//conf.set("mapred.job.tracker", "local"); // For debug.
		

			
			 ExecutorService executor = Executors.newSingleThreadExecutor();
			 MetaServer seqServer = new MetaServer(conf);
			 seqServer.start();
			 
			 if (dimTransfers.size()>0){
				 //executor.submit(new DimensionJobHandler(dimTransfers, conf));
				 executor.submit(new BigDimensionJobHandler(dimTransfers, conf));
			 }
			 
			 if (factTransfers.size()>0)
				 executor.submit(new FactJobHandler(factTransfers, conf));
			 
			 executor.shutdown();
			 executor.awaitTermination(1000*3600, TimeUnit.SECONDS);
			 
			 seqServer.cease();
			 
			 
			/*boolean succeedInDimProcessing = true;
			if (dimTransfers.size()>0){
				MetaServer seqServer = new MetaServer(conf);
				seqServer.start();
				succeedInDimProcessing = new DimensionJobHandler().handle(dimTransfers, conf);
				seqServer.cease();
			}
			if (factTransfers.size()>0 && succeedInDimProcessing){
				new FactJobHandler().handle(factTransfers, conf);
			}
			*/

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			// System.exit(-1);
		}
	}

	public static final JobPlanner DEFAULT = new JobPlanner();
}
