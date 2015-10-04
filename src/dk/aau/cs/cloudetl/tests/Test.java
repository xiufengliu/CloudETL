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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import dk.aau.cs.cloudetl.common.Utils;
import dk.aau.cs.cloudetl.io.SCDValueWritable;

public class Test {

	public void testBlockLocations(String relativePath) {
		Path path = new Path("hdfs://apu:54310"+relativePath);
		try {
			FileSystem fs = path.getFileSystem(new Configuration());
			BlockLocation[] blks = fs.getFileBlockLocations(path, 0,
					Long.MAX_VALUE);

			StringBuffer buf = new StringBuffer();
			for (BlockLocation blk : blks) {
				String[] names = blk.getNames();
				buf.setLength(0);
				for (String name : names) {
					buf.append(name).append(",");
				}
				System.out.println(buf.toString());
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	
	
	public void testSubstr() {
		String url = "/user/cloudetl/input/pages/pages.csv";
		int idx = url.lastIndexOf(Path.SEPARATOR);
		System.out.println(url.substring(idx+1));
	}

	public void testRegex(){
		String targetName = "pagepart_1.csv";
		String pattern = "pagepart_1.csv";
		Pattern p = Pattern.compile(".*" + pattern + ".*");
		if (p.matcher(targetName).matches()) {
			System.out.println("True");
		} else {
			System.out.println("False");
		}
	}
	
	public void testMapOrderByValues(){
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put("130.225.195.3:50010", 10);
		map.put("130.225.195.1:50010", 12);
		map.put("130.225.195.2:50010", 16);
		map.put("130.225.195.4:50010", 11);
		map.put("130.225.195.5:50010", 23);
		
		PriorityQueue<Map.Entry<String,Integer>> q = new PriorityQueue<Map.Entry<String,Integer>>(
			    50,
			    new Comparator<Map.Entry<String, Integer>>() {
					@Override
					public int compare(Entry<String, Integer> e1,
							Entry<String, Integer> e2) {
						return e2.getValue().compareTo(e1.getValue());
					}
			    });
		
		q.addAll(map.entrySet());
		
		while(!q.isEmpty()){
			Entry<String, Integer> entry= q.poll();
			System.out.println(entry.getKey()+"="+entry.getValue());
		}
		
		
	}
	
	public static void reverse(String str){
		//StringBuffer buf = new StringBuffer(str);
		//return buf.reverse().toString();
		char[] chrArray =  str.toCharArray();
		int len = chrArray.length;
		
		for (int j=(len-1-1)>>1; j>=0; --j){
			char tmp1 = chrArray[j];
			char tmp2 = chrArray[len-j];
			
			 chrArray[j] = tmp2;
			 chrArray[len-j] = tmp1;
		}
		System.out.println(new String(chrArray));
		
	}
	
	public void regexPattern(){
		String url = "http://www.domain000.tl0000/page06966.html";
		String serverver = "PowerServer/2.0";
		String EXAMPLE_TEST = "This is my small example "
			      + "string which I'm going to " + "use for pattern matching.";
		
		String regex2 = "(\\w*\\.\\w*\\.\\w*)";
		Pattern pattern =  Pattern.compile(regex2);
		Matcher m = pattern.matcher(url);
		while (m.find()){
			System.out.println(m.group());
		}
		
	}
	
	public static void main(final String[] args) {
		Test test = new Test();
		//test.reverse("Hello world");
		test.regexPattern();
	}
	
	class ValueComparator implements Comparator {

		  Map base;
		  public ValueComparator(Map base) {
		      this.base = base;
		  }

		  public int compare(Object a, Object b) {

		    if((Integer)base.get(a) < (Integer)base.get(b)) {
		      return 1;
		    } else if((Integer)base.get(a) == (Integer)base.get(b)) {
		      return 0;
		    } else {
		      return -1;
		    }
		  }
		}
	

}