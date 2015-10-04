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
package dk.aau.cs.cloudetl.common;

import java.io.File;

public interface CEConstants {
	  
	  
	static final String CLOUDETL_CONFIG = "conf" + File.separator+ "cloudetl.xml";
	static final String CLOUDETL_HOME = "cloudetl.home";  
	  
	  
	  static final String META_DIR = "cloudetl.meta";
	  static final String LIB_DIR = "cloudetl.lib";

	  static final int SEQ_SERVER_PORT = 9250;
	  
	  
	  static final String SEQ_INCR_DELTA = "cloudetl.seq.incr.delta";
	  
	  static final String CLOUDETL_HDFS_TMP_DIR = "cloudetl.hdfs.tmp";
	  static final String CLOUDETL_HDFS_LIB_DIR = "cloudetl.hdfs.lib";
	  
	  static final String CLOUDETL_LOOKUP_INDEX_DIR = "cloudetl.lookups";
	  
	  static final byte IS_NULL = 1;
	  static final byte IS_NOT_NULL = 0;
	  
	  
	  
	  
	  
	  
	  
	  static final String DIMENSION_PROCESSING = "Dimension Processing";
	  static final String FACT_PROCESSING = "Fact Processing";
	  
	  
	  static final String NAMED_OUTPUT_TEXT = "namedOutputText";
	  static final String NAMED_OUTPUT_MAP = "namedOutputMap";
	  static final String NAMED_OUTPUT_SCD_MAP = "namedOutputSCDMap";
	  
	
	  
	  //=====================================================
	  /*static final String CLOUDETL_HDFS_DATA_DIR = "cloudetl/data";
	  static final String CLOUDETL_HDFS_RESULTS_DIR = "cloudetl/results";
	  
	  static final String DEFAULT_DATE_FORMAT = "yyyy/MM/dd";
	  static final String DEFAULT_TIME_FORMAT = "HH:mm:ss";
	  static final String DEFAULT_DAYNAME_FORMAT = "EEEE";
	  static final String DEFAULT_MONTHNAME_FORMAT = "MMMM";
	  static final String DEFAULT_TIMESTAMP_FORMAT = "yyyy/MM/dd HH:mm:ss";*/
}
