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
package dk.aau.cs.cloudetl.hadoop.fs;

import java.io.IOException;
import java.io.PrintStream;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

/**
 * Class to help read query result.
 */
class ResultReader extends Configured
{
  private Configuration conf;
  private BufferedReader currReader;
  private LinkedList<Path> filesQueue;

  ResultReader( Path resultDir)
    throws IOException
  {
    currReader = null;
    conf = new Configuration( );
    FileSystem fs = FileSystem.get( conf);
    filesQueue = new LinkedList<Path>( );

    Path[] files = FSUtil.getFiles( resultDir, conf);

    if( files != null)
    {
      for( int i = 0; i < files.length; i++)
      {
        filesQueue.add( files[i]);
      }
    }

    currReader = getNextFileReader( );
  }

  BufferedReader getNextFileReader( )
    throws IOException
  {
    if( currReader != null)
      currReader.close( );

    try
    {
      Path path = filesQueue.removeFirst( );
      FileSystem fs = FileSystem.get( conf);
      FSDataInputStream in = fs.open( path);
      InputStreamReader inReader = new InputStreamReader( in);
      currReader = new BufferedReader( inReader);
    }
    catch( NoSuchElementException e) 
    {
      currReader = null;
    }
    
    return currReader;
  }

  BufferedReader getCurrentReader( )
  {
    return currReader;
  }
}
