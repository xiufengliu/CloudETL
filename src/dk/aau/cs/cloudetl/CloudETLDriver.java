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
package dk.aau.cs.cloudetl;


import org.apache.hadoop.util.ProgramDriver;

import dk.aau.cs.cloudetl.metadata.MetaServer;
import dk.aau.cs.cloudetl.prepartition.Prepartitioner;
import dk.aau.cs.cloudetl.tests.CloudETLBigdimTest;
import dk.aau.cs.cloudetl.tests.CloudETLStarNoSCDTest;
import dk.aau.cs.cloudetl.tests.CloudETLStarWithSCDTest;
import dk.aau.cs.cloudetl.tests.Test;

/**
 * A description of an example program based on its class and a 
 * human-readable description.
 */
public class CloudETLDriver {
  
  public static void main(String argv[]){
    int exitCode = -1;
    ProgramDriver pgd = new ProgramDriver();
    try {
      pgd.addClass("prepartition", Prepartitioner.class, "Pre-partition the data files froms, and co-locate the files in the HDFS");
      pgd.addClass("scd", CloudETLStarWithSCDTest.class,   "Load dims with SCD and facts in CloudETL");
      pgd.addClass("noscd", CloudETLStarNoSCDTest.class,   "Load dims No SCD and facts in CloudETL");
      pgd.addClass("bigdim", CloudETLBigdimTest.class,   "Load dims No SCD and facts in CloudETL");
      pgd.addClass("mytest", Test.class,   "My testing during development");  
      pgd.addClass("seqserver", MetaServer.class,   "My testing during development");
      exitCode = pgd.driver(argv);
    }
    catch(Throwable e){
      e.printStackTrace();
    }
    
    System.exit(exitCode);
  }
}
	
