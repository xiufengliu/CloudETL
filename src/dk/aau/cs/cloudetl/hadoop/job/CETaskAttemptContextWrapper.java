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

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

public class CETaskAttemptContextWrapper<E> implements TaskAttemptContext {
	E e;

	TaskAttemptContext ctx;

	public CETaskAttemptContextWrapper(TaskAttemptContext ctx) {
		this.ctx = ctx;
	}

	public void set(E e) {
		this.e = e;
	}

	public E get() {
		return e;
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

	@Override
	public Class<?> getOutputKeyClass() {
		return ctx.getOutputKeyClass();
	}

	@Override
	public Class<?> getOutputValueClass() {
		return ctx.getOutputValueClass();
	}

}