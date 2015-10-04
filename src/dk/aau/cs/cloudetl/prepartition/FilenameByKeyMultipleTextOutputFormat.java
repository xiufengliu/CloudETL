package dk.aau.cs.cloudetl.prepartition;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * A class which outputs values to files named as the key i.e. the key is the
 * filename and the value is written in a line in the file
 *
 *
 */
public class FilenameByKeyMultipleTextOutputFormat<K, V> extends
                TextOutputFormat<K, V> {
        protected static class FilenameByKeyMultipleTextOutputFormaLineRecordWriter<K, V>
                        extends RecordWriter<K, V> {
                private static final String utf8 = "UTF-8";
                private static final byte[] newline;
                // Static newline initialization
                static {
                        try {
                                newline = "\n".getBytes(utf8);
                        } catch (UnsupportedEncodingException uee) {
                                throw new IllegalArgumentException("can't find " + utf8
                                                + " encoding");
                        }
                }
                protected TaskAttemptContext job;
                protected CompressionCodec codec;
                protected String extension = "";
                protected Map<String, DataOutputStream> outMap;

                /**
                 * Class constructor
                 *
                 * @param codec
                 *            the compression codec
                 * @param keyValueSeparator
                 *            the key value separator string
                 * @param job
                 *            the {@link TaskAttemptContext}
                 */
                public FilenameByKeyMultipleTextOutputFormaLineRecordWriter(
                                CompressionCodec codec, TaskAttemptContext job) {
                        this.job = job;
                        this.codec = codec;
                        if (null != codec)
                                this.extension = codec.getDefaultExtension();
                        outMap = new HashMap<String, DataOutputStream>();
                }

                /**
                 * Write the object to the byte stream, handling Text as a special case.
                 *
                 * @param o
                 *            the object to print
                 * @throws IOException
                 *             if the write throws, we pass it on
                 */
                private void writeObject(Object o, DataOutputStream out)
                                throws IOException {
                        if (o instanceof Text) {
                                Text to = (Text) o;
                                out.write(to.getBytes(), 0, to.getLength());
                        } else {
                                out.write(o.toString().getBytes(utf8));
                        }
                }

                /**
                 * The method to write key value pairs
                 *
                 * @param key
                 *            the key
                 * @param value
                 *            the value
                 */
                @Override
                public synchronized void write(K key, V value) throws IOException {

                        boolean nullKey = key == null || key instanceof NullWritable;
                        boolean nullValue = value == null || value instanceof NullWritable;
                        if (nullKey || nullValue) {
                                return;
                        }
                        // Get the stream
                        String sOutputFile = key.toString();
                        DataOutputStream out = outMap.get(sOutputFile);
                        if (null == out) { // The stream is not there, it has to be created
                                Path file = new Path(job.getConfiguration().get(
                                                "mapred.output.dir"), sOutputFile);
                                FileSystem fs = file.getFileSystem(job.getConfiguration());
                                FSDataOutputStream fileOut = null;
                                try {
                                        fileOut = fs.create(file, true);
                                } catch (IOException e) {
                                        fileOut = fs.append(file);
                                }
                                outMap.put(sOutputFile, fileOut);
                                out = fileOut;
                        }
                        // No need to write the key-value separator, just write the value
                        // and print newline
                        writeObject(value, out);
                        out.write(newline);
                }

                /**
                 * @param context
                 *            the TaskAttemptContext
                 */
                @Override
                public synchronized void close(TaskAttemptContext context)
                                throws IOException {
                        Iterator<DataOutputStream> iter = outMap.values().iterator();
                        // Iterate through all the DataOutputStream objects and close them
                        while (iter.hasNext())
                                iter.next().close();
                }
        }

        /**
         * @param job
         *            the TaskAttemptContext
         */
        @Override
        public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job)
                        throws IOException, InterruptedException {
                Configuration conf = job.getConfiguration();
                boolean isCompressed = getCompressOutput(job);
                CompressionCodec codec = null;
                if (isCompressed) {
                        Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
                                        job, GzipCodec.class);
                        codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
                                        conf);
                }
                return new FilenameByKeyMultipleTextOutputFormaLineRecordWriter<K, V>(
                                codec, job);
        }
}