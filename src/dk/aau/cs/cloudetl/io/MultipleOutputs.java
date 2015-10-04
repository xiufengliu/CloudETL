package dk.aau.cs.cloudetl.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.OutputCollector;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import dk.aau.cs.cloudetl.hadoop.job.DimensionJobHandler;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * The MultipleOutputs class simplifies writting to additional outputs other
 * than the job default output via the <code>OutputCollector</code> passed to
 * the <code>map()</code> and <code>reduce()</code> methods of the
 * <code>Mapper</code> and <code>Reducer</code> implementations.
 * <p/>
 * Each additional output, or named output, may be configured with its own
 * <code>OutputFormat</code>, with its own key class and with its own value
 * class.
 * <p/>
 * A named output can be a single file or a multi file. The later is refered as
 * a multi named output.
 * <p/>
 * A multi named output is an unbound set of files all sharing the same
 * <code>OutputFormat</code>, key class and value class configuration.
 * <p/>
 * When named outputs are used within a <code>Mapper</code> implementation,
 * key/values written to a name output are not part of the reduce phase, only
 * key/values written to the job <code>OutputCollector</code> are part of the
 * reduce phase.
 * <p/>
 * MultipleOutputs supports counters, by default the are disabled. The counters
 * group is the {@link MultipleOutputs} class name.
 * </p>
 * The names of the counters are the same as the named outputs. For multi named
 * outputs the name of the counter is the concatenation of the named output, and
 * underscore '_' and the multiname.
 * <p/>
 * Job configuration usage pattern is:
 * 
 * <pre>
 * 
 * JobConf conf = new JobConf();
 * 
 * conf.setInputPath(inDir);
 * FileOutputFormat.setOutputPath(conf, outDir);
 * 
 * conf.setMapperClass(MOMap.class);
 * conf.setReducerClass(MOReduce.class);
 * ...
 * 
 * // Defines additional single text based output 'text' for the job
 * MultipleOutputs.addNamedOutput(conf, "text", TextOutputFormat.class,
 * LongWritable.class, Text.class);
 * 
 * // Defines additional multi sequencefile based output 'sequence' for the
 * // job
 * MultipleOutputs.addMultiNamedOutput(conf, "seq",
 *   SequenceFileOutputFormat.class,
 *   LongWritable.class, Text.class);
 * ...
 * 
 * JobClient jc = new JobClient();
 * RunningJob job = jc.submitJob(conf);
 * 
 * ...
 * </pre>
 * <p/>
 * Job configuration usage pattern is:
 * 
 * <pre>
 * 
 * public class MOReduce implements Reducer&lt;WritableComparable, Writable&gt; {
 * 	private MultipleOutputs mos;
 * 
 * 	public void configure(JobConf conf) {
 * ...
 * mos = new MultipleOutputs(conf);
 * }
 * 
 * 	public void reduce(WritableComparable key, Iterator&lt;Writable&gt; values,
 * OutputCollector output, Reporter reporter)
 * throws IOException {
 * ...
 * mos.getCollector(&quot;text&quot;, reporter).collect(key, new Text(&quot;Hello&quot;));
 * mos.getCollector(&quot;seq&quot;, &quot;A&quot;, reporter).collect(key, new Text(&quot;Bye&quot;));
 * mos.getCollector(&quot;seq&quot;, &quot;B&quot;, reporter).collect(key, new Text(&quot;Chau&quot;));
 * ...
 * }
 * 
 * 	public void close() throws IOException {
 * mos.close();
 * ...
 * }
 * }
 * </pre>
 */
public class MultipleOutputs {
	private static final Log log = LogFactory.getLog(MultipleOutputs.class);

	private static final String NAMED_OUTPUTS = "mo.namedOutputs";

	private static final String MO_PREFIX = "mo.namedOutput.";

	private static final String FORMAT = ".format";
	private static final String KEY = ".key";
	private static final String VALUE = ".value";
	private static final String MULTI = ".multi";

	private static final String COUNTERS_ENABLED = "mo.counters";

	/**
	 * Counters group used by the counters of MultipleOutputs.
	 */
	private static final String COUNTERS_GROUP = MultipleOutputs.class.getName();

	
	// instance code, to be used from Mapper/Reducer code

	private Configuration conf;
	// private Job job;
	private OutputFormat outputFormat;
	private Set<String> namedOutputs;
	private Map<String, RecordWriter> recordWriters;
	private boolean countersEnabled;


	
	
	/**
	 * Creates and initializes multiple named outputs support, it should be
	 * instantiated in the Mapper/Reducer setup method.
	 * 
	 * @param context
	 *            the Context passed to setup
	 */
	@SuppressWarnings("unchecked")
	public MultipleOutputs(TaskInputOutputContext context) {
		this.conf = context.getConfiguration();

		outputFormat = new InternalFileOutputFormat();
		
		namedOutputs = Collections.unmodifiableSet(new HashSet<String>(MultipleOutputs.getNamedOutputsList(this.conf)));
		recordWriters = new HashMap<String, RecordWriter>();
		countersEnabled = getCountersEnabled(this.conf);
	}

	
	/**
	 * Checks if a named output is alreadyDefined or not.
	 * 
	 * @param conf
	 *            job conf
	 * @param namedOutput
	 *            named output names
	 * @param alreadyDefined
	 *            whether the existence/non-existence of the named output is to
	 *            be checked
	 * @throws IllegalArgumentException
	 *             if the output name is alreadyDefined or not depending on the
	 *             value of the 'alreadyDefined' parameter
	 */
	private static void checkNamedOutput(Configuration conf,
			String namedOutput, boolean alreadyDefined) {
		List<String> definedChannels = getNamedOutputsList(conf);
		if (alreadyDefined && definedChannels.contains(namedOutput)) {
			throw new IllegalArgumentException("Named output '" + namedOutput
					+ "' already alreadyDefined");
		} else if (!alreadyDefined && !definedChannels.contains(namedOutput)) {
			throw new IllegalArgumentException("Named output '" + namedOutput
					+ "' not defined");
		}
	}

	/**
	 * Checks if a named output name is valid token.
	 * 
	 * @param namedOutput
	 *            named output Name
	 * @throws IllegalArgumentException
	 *             if the output name is not valid.
	 */
	private static void checkTokenName(String namedOutput) {
		if (namedOutput == null || namedOutput.length() == 0) {
			throw new IllegalArgumentException("Name cannot be NULL or emtpy");
		}
		for (char ch : namedOutput.toCharArray()) {
			if ((ch >= 'A') && (ch <= 'Z')) {
				continue;
			}
			if ((ch >= 'a') && (ch <= 'z')) {
				continue;
			}
			if ((ch >= '0') && (ch <= '9')) {
				continue;
			}
			throw new IllegalArgumentException("Name cannot be have a '" + ch
					+ "' char");
		}
	}

	/**
	 * Checks if a named output name is valid.
	 * 
	 * @param namedOutput
	 *            named output Name
	 * @throws IllegalArgumentException
	 *             if the output name is not valid.
	 */
	private static void checkNamedOutputName(String namedOutput) {
		checkTokenName(namedOutput);
		// name cannot be the name used for the default output
		if (namedOutput.equals("part")) {
			throw new IllegalArgumentException(
					"Named output name cannot be 'part'");
		}
	}

	/**
	 * Returns list of channel names.
	 * 
	 * @param conf
	 *            job conf
	 * @return List of channel Names
	 */
	public static List<String> getNamedOutputsList(Configuration conf) {
		List<String> names = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(conf.get(NAMED_OUTPUTS, ""),
				" ");
		while (st.hasMoreTokens()) {
			names.add(st.nextToken());
		}
		return names;
	}

	/**
	 * Returns if a named output is multiple.
	 * 
	 * @param conf
	 *            job conf
	 * @param namedOutput
	 *            named output
	 * @return <code>true</code> if the name output is multi, <code>false</code>
	 *         if it is single. If the name output is not defined it returns
	 *         <code>false</code>
	 */
	public static boolean isMultiNamedOutput(Configuration conf,
			String namedOutput) {
		checkNamedOutput(conf, namedOutput, false);
		return conf.getBoolean(MO_PREFIX + namedOutput + MULTI, false);
	}

	/**
	 * Returns the named output OutputFormat.
	 * 
	 * @param conf
	 *            job conf
	 * @param namedOutput
	 *            named output
	 * @return namedOutput OutputFormat
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends OutputFormat> getNamedOutputFormatClass(
			Job job, String namedOutput) {
		checkNamedOutput(job.getConfiguration(), namedOutput, false);
		return job.getConfiguration().getClass(
				MO_PREFIX + namedOutput + FORMAT, null, OutputFormat.class);
	}

	/**
	 * Returns the key class for a named output.
	 * 
	 * @param conf
	 *            job conf
	 * @param namedOutput
	 *            named output
	 * @return class for the named output key
	 */
	@SuppressWarnings("unchecked")
	public static Class<? extends WritableComparable> getNamedOutputKeyClass(
			Job job, String namedOutput) {
		checkNamedOutput(job.getConfiguration(), namedOutput, false);
		return job.getConfiguration().getClass(MO_PREFIX + namedOutput + KEY,
				null, WritableComparable.class);
	}

	/**
	 * Returns the value class for a named output.
	 * 
	 * @param conf
	 *            job conf
	 * @param namedOutput
	 *            named output
	 * @return class of named output value
	 */
	public static Class<? extends Writable> getNamedOutputValueClass(Job job,
			String namedOutput) {
		checkNamedOutput(job.getConfiguration(), namedOutput, false);
		return job.getConfiguration().getClass(MO_PREFIX + namedOutput + VALUE,
				null, Writable.class);
	}

	/**
	 * Adds a named output for the job.
	 * <p/>
	 * 
	 * @param conf
	 *            job conf to add the named output
	 * @param namedOutput
	 *            named output name, it has to be a word, letters and numbers
	 *            only, cannot be the word 'part' as that is reserved for the
	 *            default output.
	 * @param outputFormatClass
	 *            OutputFormat class.
	 * @param keyClass
	 *            key class
	 * @param valueClass
	 *            value class
	 */
	/*
	 * @SuppressWarnings("unchecked") public static void addNamedOutput(Job job,
	 * String namedOutput, Class<? extends FileOutputFormat> outputFormatClass,
	 * Class<?> keyClass, Class<?> valueClass) { addNamedOutput(job,
	 * namedOutput, false, outputFormatClass, keyClass, valueClass); }
	 */

	/**
	 * Adds a multi named output for the job.
	 * <p/>
	 * 
	 * @param conf
	 *            job conf to add the named output
	 * @param namedOutput
	 *            named output name, it has to be a word, letters and numbers
	 *            only, cannot be the word 'part' as that is reserved for the
	 *            default output.
	 * @param outputFormatClass
	 *            OutputFormat class.
	 * @param keyClass
	 *            key class
	 * @param valueClass
	 *            value class
	 */
	@SuppressWarnings("unchecked")
	public static void addMultiNamedOutput(Job job, String namedOutput,	Class<? extends FileOutputFormat> outputFormatClass, Class<?> keyClass, Class<?> valueClass) {
		checkNamedOutputName(namedOutput);
		checkNamedOutput(job.getConfiguration(), namedOutput, true);
		Configuration conf = job.getConfiguration();
		conf.set(NAMED_OUTPUTS, conf.get(NAMED_OUTPUTS, "") + " " + namedOutput);
		conf.setClass(MO_PREFIX + namedOutput + FORMAT, outputFormatClass,	OutputFormat.class);
		conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
		conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
		conf.setBoolean(MO_PREFIX + namedOutput + MULTI, true);
	}

	/**
	 * Adds a named output for the job.
	 * <p/>
	 * 
	 * @param conf
	 *            job conf to add the named output
	 * @param namedOutput
	 *            named output name, it has to be a word, letters and numbers
	 *            only, cannot be the word 'part' as that is reserved for the
	 *            default output.
	 * @param multi
	 *            indicates if the named output is multi
	 * @param outputFormatClass
	 *            OutputFormat class.
	 * @param keyClass
	 *            key class
	 * @param valueClass
	 *            value class
	 */
	/*
	 * private static void addNamedOutput(Job job, String namedOutput, boolean
	 * multi, Class<? extends OutputFormat> outputFormatClass, Class<?>
	 * keyClass, Class<?> valueClass) { checkNamedOutputName(namedOutput);
	 * checkNamedOutput(job.getConfiguration(), namedOutput, true);
	 * Configuration conf = job.getConfiguration(); conf.set(NAMED_OUTPUTS,
	 * conf.get(NAMED_OUTPUTS, "") + " " + namedOutput); conf.setClass(MO_PREFIX
	 * + namedOutput + FORMAT, outputFormatClass, OutputFormat.class);
	 * conf.setClass(MO_PREFIX + namedOutput + KEY, keyClass, Object.class);
	 * conf.setClass(MO_PREFIX + namedOutput + VALUE, valueClass, Object.class);
	 * conf.setBoolean(MO_PREFIX + namedOutput + MULTI, multi); }
	 */

	/**
	 * Enables or disables counters for the named outputs.
	 * <p/>
	 * By default these counters are disabled.
	 * <p/>
	 * MultipleOutputs supports counters, by default the are disabled. The
	 * counters group is the {@link MultipleOutputs} class name.
	 * </p>
	 * The names of the counters are the same as the named outputs. For multi
	 * named outputs the name of the counter is the concatenation of the named
	 * output, and underscore '_' and the multiname.
	 * 
	 * @param conf
	 *            job conf to enableadd the named output.
	 * @param enabled
	 *            indicates if the counters will be enabled or not.
	 */
	public static void setCountersEnabled(Job job, boolean enabled) {
		job.getConfiguration().setBoolean(COUNTERS_ENABLED, enabled);
	}

	/**
	 * Returns if the counters for the named outputs are enabled or not.
	 * <p/>
	 * By default these counters are disabled.
	 * <p/>
	 * MultipleOutputs supports counters, by default the are disabled. The
	 * counters group is the {@link MultipleOutputs} class name.
	 * </p>
	 * The names of the counters are the same as the named outputs. For multi
	 * named outputs the name of the counter is the concatenation of the named
	 * output, and underscore '_' and the multiname.
	 * 
	 * 
	 * @param conf
	 *            job conf to enableadd the named output.
	 * @return TRUE if the counters are enabled, FALSE if they are disabled.
	 */
	public static boolean getCountersEnabled(Configuration conf) {
		return conf.getBoolean(COUNTERS_ENABLED, false);
	}


	/**
	 * Returns iterator with the defined name outputs.
	 * 
	 * @return iterator with the defined named outputs
	 */
	public Iterator<String> getNamedOutputs() {
		return namedOutputs.iterator();
	}

	// by being synchronized MultipleOutputTask can be use with a
	// MultithreaderMapRunner.
	@SuppressWarnings("unchecked")
	private synchronized RecordWriter getRecordWriter(String namedOutput, String baseFileName, String multiName, TaskInputOutputContext ctx) throws IOException, InterruptedException {
		RecordWriter writer = recordWriters.get(baseFileName);
		if (writer == null) {
			if (countersEnabled && ctx == null) {
				throw new IllegalArgumentException(
						"Counters are enabled, Context cannot be NULL");
			}

			conf.set(NAMED_OUTPUTS + MULTI, multiName);// added by xiliu, which used for creating
														// multiname based
														// output file.

			conf.set(InternalFileOutputFormat.CONFIG_NAMED_OUTPUT, baseFileName);

			conf.setClass(InternalFileOutputFormat.CONFIG_NAMED_OUTPUT_CLASS, conf.getClass(MO_PREFIX + namedOutput + FORMAT, TextOutputFormat.class), OutputFormat.class);

			writer = outputFormat.getRecordWriter(new MOTaskAttemptContextWrapper(namedOutput, ctx));

			if (countersEnabled) {
				if (ctx == null) {
					throw new IllegalArgumentException(
							"Counters are enabled, Context cannot be NULL");
				}
				writer = new RecordWriterWithCounter(writer, baseFileName, ctx);
			}
			recordWriters.put(baseFileName, writer);
		}
		return writer;
	}

	private static class RecordWriterWithCounter extends RecordWriter {
		private RecordWriter writer;
		private String counterName;
		private TaskInputOutputContext ctx;

		public RecordWriterWithCounter(RecordWriter writer, String counterName,TaskInputOutputContext ctx) {
			this.writer = writer;
			this.counterName = counterName;
			this.ctx = ctx;
		}

		@SuppressWarnings({ "unchecked" })
		public void write(Object key, Object value) throws IOException,	InterruptedException {
			ctx.getCounter(COUNTERS_GROUP, counterName).increment(1);
			writer.write(key, value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			writer.close(context);
		}
	}

	/**
	 * Gets the output collector for a named output.
	 * <p/>
	 * 
	 * @param namedOutput
	 *            the named output name
	 * @param reporter
	 *            the reporter
	 * @return the output collector for the given named output
	 * @throws IOException
	 *             thrown if output collector could not be created
	 */
	@SuppressWarnings({ "unchecked" })
	public OutputCollector getCollector(String namedOutput,
			TaskInputOutputContext context) throws IOException,
			InterruptedException {
		return getCollector(namedOutput, null, context);
	}

	/**
	 * Gets the output collector for a multi named output.
	 * <p/>
	 * 
	 * @param namedOutput
	 *            the named output name
	 * @param multiName
	 *            the multi name part
	 * @param context
	 *            the context
	 * @return the output collector for the given named output
	 * @throws IOException
	 *             thrown if output collector could not be created
	 */
	@SuppressWarnings({ "unchecked" })
	public OutputCollector getCollector(String namedOutput, String multiName,	TaskInputOutputContext context) throws IOException,
			InterruptedException {

		checkNamedOutputName(namedOutput);
		if (!namedOutputs.contains(namedOutput)) {
			throw new IllegalArgumentException("Undefined named output '"
					+ namedOutput + "'");
		}

		boolean multi = isMultiNamedOutput(conf, namedOutput);

		if (!multi && multiName != null) {
			throw new IllegalArgumentException("Name output '" + namedOutput
					+ "' has not been defined as multi");
		}
		if (multi) {
			checkTokenName(multiName);
		}

		String baseFileName = (multi) ? namedOutput + "_" + multiName: namedOutput;

		final RecordWriter writer = getRecordWriter(namedOutput, baseFileName,	multiName, context);

		return new OutputCollector() {
			@SuppressWarnings({ "unchecked" })
			public void collect(Object key, Object value) throws IOException {
				try {
					writer.write(key, value);
				} catch (InterruptedException ie) {
					throw new RuntimeException(ie);
				}
			}
		};
	}

	/**
	 * Closes all the opened named outputs.
	 * <p/>
	 * If overriden subclasses must invoke <code>super.close()</code> at the end
	 * of their <code>close()</code>
	 * 
	 * @throws java.io.IOException
	 *             thrown if any of the MultipleOutput files could not be closed
	 *             properly.
	 */
	public void close(TaskInputOutputContext ctx) throws IOException, InterruptedException {
		for (RecordWriter writer : recordWriters.values()) {
			writer.close(ctx);
		}
	}

	private class InternalFileOutputFormat extends FileOutputFormat<Object, Object> {

		public static final String CONFIG_NAMED_OUTPUT = "mo.config.namedOutput";
		public static final String CONFIG_NAMED_OUTPUT_CLASS = "mo.config.namedOutput.class";

		
		@SuppressWarnings("unchecked")
		@Override
		public RecordWriter<Object, Object> getRecordWriter(TaskAttemptContext job) throws IOException,	InterruptedException {
			Configuration conf = job.getConfiguration();
			Class c = conf.getClass(CONFIG_NAMED_OUTPUT_CLASS,	TextOutputFormat.class);
			FileOutputFormat format = (FileOutputFormat) ReflectionUtils.newInstance(c, conf);
			return format.getRecordWriter(job);
		}
	}

	
	private class MOTaskAttemptContextWrapper implements TaskAttemptContext {

		private final Class<?> outputKeyClass;
		private final Class<?> outputValueClass;
		private final TaskAttemptContext ctx;

		public MOTaskAttemptContextWrapper(final String namedOutput,
				TaskAttemptContext ctx) {
			// super(ctx.getConfiguration(), ctx.getTaskAttemptID());
			this.ctx = ctx;
			outputKeyClass = conf.getClass(MO_PREFIX + namedOutput + KEY,	LongWritable.class);
			outputValueClass = conf.getClass(MO_PREFIX + namedOutput + VALUE,	Text.class);
		}

		/**
		 * Get the key class for the job output data.
		 * 
		 * @return the key class for the job output data.
		 */
		@Override
		public Class<?> getOutputKeyClass() {
			return outputKeyClass;
		}

		/**
		 * Get the value class for job outputs.
		 * 
		 * @return the value class for job outputs.
		 */
		@Override
		public Class<?> getOutputValueClass() {
			return outputValueClass;
		}

		@Override
		public Configuration getConfiguration() {

			return ctx.getConfiguration();
		}

		@Override
		public JobID getJobID() {

			return ctx.getJobID();
		}

		@Override
		public int getNumReduceTasks() {

			return ctx.getNumReduceTasks();
		}

		@Override
		public Path getWorkingDirectory() throws IOException {

			return ctx.getWorkingDirectory();
		}

		@Override
		public Class<?> getMapOutputKeyClass() {

			return ctx.getMapOutputKeyClass();
		}

		@Override
		public Class<?> getMapOutputValueClass() {

			return ctx.getMapOutputValueClass();
		}

		@Override
		public String getJobName() {

			return ctx.getJobName();
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {

			return ctx.getInputFormatClass();
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {

			return ctx.getMapperClass();
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {

			return ctx.getCombinerClass();
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {

			return ctx.getReducerClass();
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {

			return ctx.getOutputFormatClass();
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {

			return ctx.getPartitionerClass();
		}

		@Override
		public RawComparator<?> getSortComparator() {

			return ctx.getSortComparator();
		}

		@Override
		public String getJar() {

			return ctx.getJar();
		}

		@Override
		public RawComparator<?> getGroupingComparator() {

			return ctx.getGroupingComparator();
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {

			return ctx.getJobSetupCleanupNeeded();
		}

		@Override
		public boolean getProfileEnabled() {

			return ctx.getProfileEnabled();
		}

		@Override
		public String getProfileParams() {

			return ctx.getProfileParams();
		}

		@Override
		public IntegerRanges getProfileTaskRange(boolean isMap) {

			return ctx.getProfileTaskRange(isMap);
		}

		@Override
		public String getUser() {

			return ctx.getUser();
		}

		@Override
		public boolean getSymlink() {

			return ctx.getSymlink();
		}

		@Override
		public Path[] getArchiveClassPaths() {

			return ctx.getArchiveClassPaths();
		}

		@Override
		public URI[] getCacheArchives() throws IOException {

			return ctx.getCacheArchives();
		}

		@Override
		public URI[] getCacheFiles() throws IOException {

			return ctx.getCacheFiles();
		}

		@Override
		public Path[] getLocalCacheArchives() throws IOException {

			return ctx.getLocalCacheArchives();
		}

		@Override
		public Path[] getLocalCacheFiles() throws IOException {

			return ctx.getLocalCacheFiles();
		}

		@Override
		public Path[] getFileClassPaths() {

			return ctx.getFileClassPaths();
		}

		@Override
		public String[] getArchiveTimestamps() {

			return ctx.getArchiveTimestamps();
		}

		@Override
		public String[] getFileTimestamps() {

			return ctx.getFileTimestamps();
		}

		@Override
		public int getMaxMapAttempts() {

			return ctx.getMaxMapAttempts();
		}

		@Override
		public int getMaxReduceAttempts() {
			return ctx.getMaxReduceAttempts();
		}

		@Override
		public void progress() {
			ctx.progress();

		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			return ctx.getTaskAttemptID();
		}

		@Override
		public void setStatus(String msg) {
			ctx.setStatus(msg);

		}

		@Override
		public String getStatus() {

			return ctx.getStatus();
		}
	}
}