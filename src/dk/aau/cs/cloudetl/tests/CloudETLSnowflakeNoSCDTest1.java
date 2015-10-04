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
package dk.aau.cs.cloudetl.tests;



import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.filter.FieldFilter;
import dk.aau.cs.cloudetl.filter.rule.PatternMatch;
import dk.aau.cs.cloudetl.hadoop.job.JobPlanner;
import dk.aau.cs.cloudetl.io.CSVFileReader;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.DimensionTableWriter;
import dk.aau.cs.cloudetl.lookup.Lookup;
import dk.aau.cs.cloudetl.lookup.LookupTransformer;
import dk.aau.cs.cloudetl.metadata.SEQ;
import dk.aau.cs.cloudetl.transform.AddField;
import dk.aau.cs.cloudetl.transform.ExcludeFields;
import dk.aau.cs.cloudetl.transform.FieldExploder;
import dk.aau.cs.cloudetl.transform.RenameField;
import dk.aau.cs.cloudetl.transform.TransformingReader;

public class CloudETLSnowflakeNoSCDTest1 {
    
    public static void main(String[] args) throws Throwable {
    	
    	String basePath = String.format("hdfs://%s:54310", TestConfig.MASTER_NODE);
    	
    	//['localfile', 'url', 'serverversion', 'size', 'downloaddate', 'lastmoddate']
    	DataReader  pages = new CSVFileReader(basePath+"/user/cloudetl/input/pages") 
								        .setField("localfile", DataType.STRING)
								      	.setField("url", DataType.STRING)
								      	.setField("serverversion", DataType.STRING)
								      	.setField("size", DataType.INT)
								      	.setField("downloaddate", DataType.DATE)
								      	.setField("lastmoddate", DataType.DATE);
    	// ['test', ]t 
    	DataReader  tests = new CSVFileReader(basePath+"/user/cloudetl/input/tests")
								        .setField("test", DataType.STRING);
  
    	String dimOutputDir = basePath+"/user/cloudetl/dims";

	    // --------------------- serververdim, serverdim transform pipe -----------------
		DataWriter serververdim = new DimensionTableWriter(dimOutputDir,"serververdim")
								.setField("serververid", DataType.INT)
								.setField("serverversion", DataType.STRING, FieldType.LOOKUP)
								.setField("serverid", DataType.INT);
		DataWriter serverdim = new DimensionTableWriter(dimOutputDir,"serverdim")
								.setField("serverid", DataType.INT)
								.setField("server", DataType.STRING, FieldType.LOOKUP);
	
	    TransformingReader serververPipe = new TransformingReader(pages);
	    serververPipe.add(new ExcludeFields("localfile", "url", "size", "downloaddate", "lastmoddate"));
	    TransformingReader serverdimPipe = new TransformingReader(serververPipe)
	    									.add(new FieldExploder("serverversion", "^\\w*", new String[]{"server"} ))
											.add(new AddField("serverid", new SEQ("serverdim_id"), DataType.INT));
	    TransformingReader serververdimPipe = new TransformingReader(serververPipe)
	    									.add(new AddField("serververid", new SEQ("serververdim_id"), DataType.INT))
	    									.add(new LookupTransformer("serverid", new Lookup(serverdim, "server", -1)));
	
	    // --------------------- topdomaindim, domaindim transform pipe -----------------
	    DataWriter topdomaindim = new DimensionTableWriter(dimOutputDir,"topdomaindim")
								.setField("topdomainid", DataType.INT)
								.setField("topdomain", DataType.STRING, FieldType.LOOKUP);
								
		DataWriter domaindim = new DimensionTableWriter(dimOutputDir,"domaindim")
								.setField("domainid", DataType.INT)
								.setField("domain", DataType.STRING, FieldType.LOOKUP)
	    						.setField("topdomainid", DataType.INT);
	    
	    TransformingReader domainPipe = new TransformingReader(pages)
	    								.add(new ExcludeFields("localfile", "serverversion", "size", "downloaddate", "lastmoddate"))
	    								.add(new FieldExploder("url", "(\\w*\\.\\w*\\.(\\w*))", new String[]{"domain", "topdomain"}));
	    TransformingReader topdomaindimPipe = new TransformingReader(domainPipe)
										.add(new AddField("topdomainid", new SEQ("topdomaindim_id"), DataType.INT));
	    TransformingReader domaindimPipe = new TransformingReader(domainPipe)
										.add(new AddField("domainid", new SEQ("domaindim_id"), DataType.INT))
										.add(new LookupTransformer("topdomainid", new Lookup(topdomaindim, "topdomain", -1)));
	
		// --------------------- pagedim transform pipe -----------------	
	    TransformingReader pagedimPipe = new TransformingReader(pages)
									    .add(new ExcludeFields("localfile",   "downloaddate"))
									    .add(new AddField("pageid", new SEQ("pagedim_id"), DataType.INT))
									    .add(new LookupTransformer("topdomainid", new Lookup(domaindim, "domain", -1)))
	    								.add(new LookupTransformer("serververid", new Lookup(serververdim, "serverver", -1)));
	    
	    DataWriter pagedim = new DimensionTableWriter(dimOutputDir, "pagedim")
	      							.setField("pageid", DataType.INT, FieldType.PRI)
	      							.setField("url", DataType.STRING, FieldType.LOOKUP)
	      							.setField("serververid", DataType.INT)
	      							.setField("domainid", DataType.INT)
	      							.setField("size", DataType.INT)
	      							.setField("lastmoddate", DataType.STRING);
	    
	    // --------------------- datedim transform pipe -----------------
	    TransformingReader datedimPipe = new TransformingReader(pages)
	    								.add(new ExcludeFields("localfile", "url",  "serverversion", "size", "lastmoddate"))
	    								.add(new AddField("dateid", new SEQ("datedim_id"), DataType.INT))
	    								.add(new RenameField("downloaddate", "date"));
	    
	    DataWriter datedim = new DimensionTableWriter(dimOutputDir, "datedim")
								.setField("dateid", DataType.INT, FieldType.PRI)
								.setField("date", DataType.DATE, FieldType.LOOKUP)
								.setField("day", DataType.INT)
								.setField("month", DataType.INT)
								.setField("year", DataType.INT)
								.setField("week", DataType.INT)
								.setField("weekyear", DataType.INT);
	
	    // --------------------- testdim transform pipe -----------------
	    TransformingReader testdimPipe = new TransformingReader(tests)
	    								.add(new RenameField("test", "testname"))
	    								.add(new AddField("testid", new SEQ("testdim_id"), DataType.INT));
	    
	    DataWriter testdim = new DimensionTableWriter(dimOutputDir, "testdim")
								.setField("testid", DataType.INT, FieldType.PRI)
								.setField("testname", DataType.STRING, FieldType.LOOKUP);
	
	    
	    // ----------------------- Add transfers and Start the chained jobs --------------------------------------
	    // Job1
	    JobPlanner.DEFAULT.addTransfer(serverdimPipe, serverdim)
	    				  .addTransfer(topdomaindimPipe, topdomaindim)
	    				  .addTransfer(datedimPipe, datedim)
	    				  .addTransfer(testdimPipe, testdim)
	    				  .start();
	    // Job2
	    JobPlanner.DEFAULT.addTransfer(domaindimPipe, domaindim)
						  .addTransfer(serververdimPipe, serververdim)
						  .start();
	    // Job3
	    JobPlanner.DEFAULT.addTransfer(pagedimPipe, pagedim)
						  .start();
   }
}
