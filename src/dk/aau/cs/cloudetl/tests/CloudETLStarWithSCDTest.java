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
import dk.aau.cs.cloudetl.filter.FilteringReader;
import dk.aau.cs.cloudetl.filter.rule.IsNotNull;
import dk.aau.cs.cloudetl.hadoop.job.JobPlanner;
import dk.aau.cs.cloudetl.io.CSVFileReader;
import dk.aau.cs.cloudetl.io.DataReader;
import dk.aau.cs.cloudetl.io.DataWriter;
import dk.aau.cs.cloudetl.io.DimensionTableWriter;
import dk.aau.cs.cloudetl.io.FactTableWriter;
import dk.aau.cs.cloudetl.io.SlowlyChangingDimensionTableWriter;
import dk.aau.cs.cloudetl.lookup.Lookup;
import dk.aau.cs.cloudetl.lookup.LookupTransformer;
import dk.aau.cs.cloudetl.lookup.SCDLookup;
import dk.aau.cs.cloudetl.metadata.SEQ;
import dk.aau.cs.cloudetl.transform.AddField;
import dk.aau.cs.cloudetl.transform.ExcludeFields;
import dk.aau.cs.cloudetl.transform.RenameField;
import dk.aau.cs.cloudetl.transform.TransformingReader;

public class CloudETLStarWithSCDTest {
    
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
    	
    	//['localfile', 'url', 'lastmoddate', 'downloaddate', 'test', 'errors']
    	/*DataReader  testResults = new CSVFileReader(basePath+"/user/cloudetl/input/testresults")
								        .setField("localfile", DataType.STRING)
								      	.setField("url", DataType.STRING)
								      	.setField("lastmoddate", DataType.DATE)
								    	.setField("downloaddate", DataType.DATE)
								    	.setField("test", DataType.STRING)
								      	.setField("errors", DataType.INT);
        */
    	  	
    //FilteringReader pagesFilter = new FilteringReader(pages);
    //pagesFilter.add(new FieldFilter("url").addRule(new IsNotNull()));
    
    // --------------------- pagedim transform pipe -----------------	
    TransformingReader pagedimPipe = new TransformingReader(pages);
    pagedimPipe.add(new ExcludeFields("localfile", "size",  "downloaddate"));
    pagedimPipe.add(new AddField("pageid", new SEQ("pagedim_id"), DataType.INT));
    pagedimPipe.add(new RenameField("lastmoddate", "validfrom"));
    
    String dimOutputDir = basePath+"/user/cloudetl/dims";
    DataWriter pagedim = new SlowlyChangingDimensionTableWriter(dimOutputDir, "pagedim")
      							.setField("pageid", DataType.INT, FieldType.PRI)
      							.setField("url", DataType.STRING, FieldType.LOOKUP)
      							.setField("serverversion", DataType.STRING)
      							.setField("version", DataType.INT, FieldType.SCD_VERSION)
      							.setField("validfrom", DataType.DATE, FieldType.SCD_VALIDFROM)
      							.setField("validto", DataType.DATE, FieldType.SCD_VALIDTO);
    
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
    
    // --------------------- testresultsfact transform pipe -----------------
    /*TransformingReader testresultsfactPipe = new TransformingReader(testResults)
    										.add(new ExcludeFields("localfile"))
    										.add(new LookupTransformer("pageid", new SCDLookup(pagedim, "url", "lastmoddate", -1)))
    										.add(new LookupTransformer("dateid", new Lookup(datedim, "downloaddate", -1)))
    										.add(new LookupTransformer("testid", new Lookup(testdim, "test", -1)));
    
    String factOutputDir = basePath+"/user/cloudetl/fact";
    DataWriter testresultsfact = new FactTableWriter(factOutputDir, "testresultsfact")
      							.setField("pageid", DataType.INT)
      							.setField("dateid", DataType.INT)
      							.setField("testid", DataType.INT)
      							.setField("errors", DataType.INT);
    */
    
    // ----------------------- Add transfers and Start the jobs --------------------------------------
    
	    JobPlanner.DEFAULT.addTransfer(pagedimPipe, pagedim)
	    				  .addTransfer(datedimPipe, datedim)
	    				  .addTransfer(testdimPipe, testdim)
	    				  .start();
    
    	 //JobPlanner.DEFAULT.addTransfer(testresultsfactPipe, testresultsfact).start();
    
   }
}
