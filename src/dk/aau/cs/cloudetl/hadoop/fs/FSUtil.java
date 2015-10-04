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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Stack;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.io.IOUtils;



import dk.aau.cs.cloudetl.common.CEConstants;

public class FSUtil extends FileUtil {

	public static boolean exists(String pathName, Configuration conf)
			throws IOException {
		return exists(new Path(pathName), conf);
	}

	public static boolean exists(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(path);
	}


	/**
	 * Creates a directory on Hadoop File system.
	 * 
	 * @param dirName
	 *            the name of the directory to be created.
	 */
	public static void mkdir(String dirName, Configuration conf)
			throws IOException {
		Path path = new Path(dirName);
		FileSystem fs = FileSystem.get(conf);

		if (!fs.mkdirs(path))
			throw new IOException("Could not create directory: " + dirName);
	}

	/**
	 * Copies data from local file system to Hadoop file system. If localPath is
	 * a directory, it copies the files in the directory to the specified hdfs
	 * path and throws exception if hdfs path was not a directory.
	 * 
	 * @param localPath
	 *            data source location path on local file system.
	 * 
	 * @param hdfsPath
	 *            target location path on HDFS.
	 */
	public static void copyFromLocal(String localPath, String hdfsPath,
			Configuration conf) throws IOException {
		Path target = new Path(hdfsPath);
		FileSystem fs = FileSystem.get(conf);

		if (new File(localPath).isFile()) {
			Path source = new Path(localPath);
			fs.copyFromLocalFile(false, false, source, target);
		} else {
			File sourceDir = new File(localPath);
			File[] files = sourceDir.listFiles();

			if (files == null)
				throw new IOException("Could not get files list for "
						+ localPath);

			for (int i = 0; i < files.length; i++) {
				Path source = new Path(files[i].getAbsolutePath());
				fs.copyFromLocalFile(false, false, source, target);
			}
		}
	}

	public static void copyToLocal(Path hdfsPath, Path localPath,
			Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		fs.copyToLocalFile(hdfsPath, localPath);
	}

	public static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS,
			Path dst, PathFilter filter, boolean deleteSource,	boolean overwrite, Configuration conf) throws IOException {
		dst = checkDest(src.getName(), dstFS, dst, overwrite);

		if (srcFS.getFileStatus(src).isDir()) {
			checkDependencies(srcFS, src, dstFS, dst);
			if (!dstFS.mkdirs(dst)) {
				return false;
			}
			FileStatus contents[] = srcFS.listStatus(src, filter);
			for (int i = 0; i < contents.length; i++) {
				copy(srcFS, contents[i].getPath(), dstFS, new Path(dst,
						contents[i].getPath().getName()), deleteSource,
						overwrite, conf);
			}
		} else if (srcFS.isFile(src)) {
			InputStream in = null;
			OutputStream out = null;
			try {
				in = srcFS.open(src);
				out = dstFS.create(dst, overwrite);
				IOUtils.copyBytes(in, out, conf, true);
			} catch (IOException e) {
				IOUtils.closeStream(out);
				IOUtils.closeStream(in);
				throw e;
			}
		} else {
			throw new IOException(src.toString()
					+ ": No such file or directory");
		}
		if (deleteSource) {
			return srcFS.delete(src, true);
		} else {
			return true;
		}

	}

	private static Path checkDest(String srcName, FileSystem dstFS, Path dst,
			boolean overwrite) throws IOException {
		if (dstFS.exists(dst)) {
			FileStatus sdst = dstFS.getFileStatus(dst);
			if (sdst.isDir()) {
				if (null == srcName) {
					throw new IOException("Target " + dst + " is a directory");
				}
				return checkDest(null, dstFS, new Path(dst, srcName), overwrite);
			} else if (!overwrite) {
				throw new IOException("Target " + dst + " already exists");
			}
		}
		return dst;
	}

	private static void checkDependencies(FileSystem srcFS, Path src,
			FileSystem dstFS, Path dst) throws IOException {
		if (srcFS == dstFS) {
			String srcq = src.makeQualified(srcFS).toString() + Path.SEPARATOR;
			String dstq = dst.makeQualified(dstFS).toString() + Path.SEPARATOR;
			if (dstq.startsWith(srcq)) {
				if (srcq.length() == dstq.length()) {
					throw new IOException("Cannot copy " + src + " to itself.");
				} else {
					throw new IOException("Cannot copy " + src
							+ " to its subdirectory " + dst);
				}
			}
		}
	}

	/**
	 * Removes the file/directory from Hadoop file system.
	 * 
	 * @param hdfsPath
	 *            the location path on Hadoop file system to be removed.
	 */
	public static void remove(String hdfsPath, Configuration conf)
			throws IOException {
		FSUtil.remove(new Path(hdfsPath), conf);
	}

	/**
	 * Removes file/directory from Hadoop file system.
	 * 
	 * @param hdfsPath
	 *            the hdfs path to be removed.
	 */
	public static void remove(Path hdfsPath, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		int retries = 5;
		while (exists(hdfsPath, conf) && !fs.delete(hdfsPath, true)
				&& retries-- >= 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new IOException("replaceFile interrupted.");
			}
		}
		if (exists(hdfsPath, conf)) {
			throw new IOException("Unable to remove " + hdfsPath.toString());
		}

	}

	public static void remove(FileSystem fs, Path hdfsPath)
			throws IOException {
		
		int retries = 5;
		while (fs.exists(hdfsPath) && !fs.delete(hdfsPath, true)
				&& retries-- >= 0) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				throw new IOException("replaceFile interrupted.");
			}
		}
		if (fs.exists(hdfsPath)) {
			throw new IOException("Unable to remove " + hdfsPath.toString());
		}

	}
	
	/**
	 * Returns the list of files (as <code>Path</code>) present in the given
	 * HDFS path.
	 * 
	 * @param path
	 *            the path on HDFS
	 * 
	 * @return an array of <code>Path</code> present in the given path.
	 */
	public static Path[] getFiles(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatus = fs.listStatus(path);
		if (fileStatus != null) {
			int numItems = fileStatus.length;
			ArrayList<Path> files = new ArrayList<Path>();
			for (int i = 0; i < numItems; i++) {
				if (!fileStatus[i].isDir())
					files.add(fileStatus[i].getPath());
			}
			Path[] tmp = new Path[files.size()];
			return files.size() == 0 ? null : files.toArray(tmp);
		} else {
			return null;
		}
	}

	/**
	 * Opens the file located at the given <code>Path</code> in HDFS and returns
	 * <code>BufferedReader</code> object for it.
	 * 
	 * @param path
	 *            the file in HDFS to be read as BufferedReader.
	 */
	public static BufferedReader getBufferedReader(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);
		InputStreamReader inReader = new InputStreamReader(in);
		BufferedReader bufReader = new BufferedReader(inReader);
		return bufReader;
	}

	/**
	 * Moves all the files present in source path on HDFS to target path on
	 * HDFS.
	 * 
	 * @param source
	 *            the source directory on HDFS
	 * 
	 * @param target
	 *            the target direcotry on HDFS. Target Dir should exist
	 */
	public static void moveFiles(Path source, Path target, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path[] files = getFiles(source, conf);

		if (files != null) {
			for (int i = 0; i < files.length; i++) {
				fs.rename(files[i], target);
			}
		}
	}

	public static Path getQualifiedPath(String pathString, Configuration conf)
			throws IOException {
		return new Path(pathString).makeQualified(FileSystem.get(conf));
	}

	public static void rename(Path src, Path target, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		if (exists(src, conf) && !fs.rename(src, target)) {
			int retries = 5;
			while (exists(target, conf) && !fs.delete(target, true)
					&& retries-- >= 0) {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					throw new IOException("replaceFile interrupted.");
				}
			}
			if (!fs.rename(src, target)) {
				throw new IOException("Unable to rename " + src + " to "
						+ target);
			}
		}
	}

	public static Path getTempPath(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		StringBuffer buf = new StringBuffer();
		buf.append(FSUtil.getDefaultFileSystemURIScheme(conf).toString())
				.append(Path.SEPARATOR)
				.append(conf.get(CEConstants.CLOUDETL_HDFS_TMP_DIR));
		return FSUtil.getQualifiedPath(buf.toString(), conf);
	}

	public static Path getLocalTempPath() throws IOException {
		File temp = File.createTempFile("map", ".tmp");
		String absolutePath = temp.getAbsolutePath();
		 String tempFilePath = absolutePath.substring(0,absolutePath.lastIndexOf(File.separator));
		temp.delete();
		//temp.mkdir();
		StringBuffer buf = new StringBuffer().append("file:///").append(tempFilePath);
		return new Path(buf.toString());
	}

	public static String makeTemporaryPathDir(String name, Configuration conf)
			throws IOException {
		if (name == null || name.isEmpty())
			name = "temp-path";
		Path tmpPath = FSUtil.getTempPath(conf);
		StringBuffer buf = new StringBuffer().append(tmpPath)
				.append(Path.SEPARATOR).append(FSUtil.getTempName(name));
		return buf.toString();
	}

	public static String getTempName(String name) {
		return name.replaceAll("[\\W\\s]+", "_")
				+ Integer.toString((int) (10000000 * Math.random()));
	}

	public static URI makeURIScheme(String stringPath, Configuration conf)
			throws IOException {
		try {
			URI uriScheme = null;

			URI uri = new URI(stringPath);
			String schemeString = uri.getScheme();
			String authority = uri.getAuthority();

			if (schemeString != null && authority != null)
				uriScheme = new URI(schemeString + "://" + authority);
			else if (schemeString != null)
				uriScheme = new URI(schemeString + ":///");
			else
				uriScheme = getDefaultFileSystemURIScheme(conf);

			return uriScheme;
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}
	}

	public static URI getDefaultFileSystemURIScheme(Configuration conf)
			throws IOException {
		return FileSystem.get(conf).getUri();
	}

	/**
	 * Returns the number of lines in the specified path on HDFS.
	 * <p>
	 * <b>NOTE:</b> If path is a directory, then the lines from the files
	 * present in the direcotry are counted.
	 * <p>
	 * <b>NOTE:</b> It does not recurse.
	 */
	public static long getNumLines(Path path, Configuration conf)
			throws IOException {
		long numLines = 0;
		FileSystem fs = FileSystem.get(conf);

		if (fs.isDirectory(path)) {
			Path[] files = getFiles(path, conf);

			if (files == null)
				return 0;

			for (int i = 0; i < files.length; i++) {
				numLines += getLines(files[i], conf);
			}
		} else {
			numLines = getLines(path, conf);
		}

		return numLines;
	}

	/**
	 * Returns the number of lines in a file.
	 */
	public static long getLines(Path path, Configuration conf)
			throws IOException {
		long numLines = 0;
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);
		InputStreamReader inReader = new InputStreamReader(in);
		BufferedReader bufReader = new BufferedReader(inReader);

		while (bufReader.readLine() != null) {
			numLines++;
		}
		return numLines;
	}

	public static FSDataOutputStream createFile(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(path, true);
		return out;
	}

	public static void ensurePath(Path path, Configuration conf)
			throws IOException {
		if (!FSUtil.exists(path, conf)) {
			FSUtil.createFile(path, conf);
		}
	}

	/**
	 * Creates a file in the given directory.
	 * 
	 * @param dirName
	 *            the name of directory on HDFS
	 * 
	 * @return <code>FSDataOutputStream</code> for the newly created file.
	 */
	public static FSDataOutputStream createFileInDir(String dirName,
			Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);

		Path filePath = new Path(dirName + java.io.File.separator + "tmp-"
				+ Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

		FSDataOutputStream stream = fs.create(filePath);
		return stream;
	}

	public static void copy(Path path1, Path path2, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileUtil.copy(fs, path1, fs, path2, false, conf);
	}

	public static void copy(Path path, OutputStream out, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(path);

		try {
			IOUtils.copyBytes(in, out, conf, false);
		} finally {
			in.close();
		}
	}

	public static void copyTopNRows(Path sourceDir, Path targetPath,
			int numRows, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path[] files = getFiles(sourceDir, conf);
		TreeMap<Long, Path> fileMap = new TreeMap<Long, Path>();

		if (files == null)
			return;

		// Reducer can output multiple files, so sort them based on their
		// modification time and pick the file that was created first.
		// POTENTIAL BUG: if the value of N is huge, then there might be a need
		// to read other files
		for (int i = 0; i < files.length; i++) {
			Path path = files[i];
			FileStatus fileStatus = fs.getFileStatus(path);
			long modificationTime = fileStatus.getModificationTime();
			fileMap.put(new Long(modificationTime), path);
		}

		Path sourcePath = fileMap.get(fileMap.firstKey());
		FSDataInputStream in = fs.open(sourcePath);
		InputStreamReader inReader = new InputStreamReader(in);
		BufferedReader bufReader = new BufferedReader(inReader);
		FSDataOutputStream out = fs.create(targetPath, true);
		PrintStream outStream = new PrintStream(out);
		String line = null;
		int count = 0;

		while (count < numRows && (line = bufReader.readLine()) != null) {
			outStream.println(line);
			count++;
		}

		outStream.close();
		bufReader.close();
	}

	public static void addJarsToClassPath(Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path homeDir = fs.getHomeDirectory();

		// first copy the jars from $CLOUDETL_HOME/lib on to hadoop file system,
		if (!exists(conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR), conf)) {
			mkdir(conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR), conf);
			Path[] files = getFiles(
					new Path(conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR)), conf);

			if (files.length == 0)
				copyFromLocal(new Path(conf.get(CEConstants.CLOUDETL_HOME), conf.get(CEConstants.LIB_DIR)).toString(),
						conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR), conf);
		}

		// get list of jar files and to distibuted cache
		Path[] files = getFiles(new Path(conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR)),
				conf);

		for (int i = 0; i < files.length; i++) {
			Path jarPath = files[i];

			// TODO Distributed cache requires absolute path-
			// /user/username/cloudbase/lib/jdom.jar
			// there should be a better way to get absolute path from relative
			// path
			String str = Path.SEPARATOR + "user" + Path.SEPARATOR
					+ homeDir.getName() + Path.SEPARATOR
					+ conf.get(CEConstants.CLOUDETL_HDFS_LIB_DIR) + Path.SEPARATOR
					+ jarPath.getName();

			DistributedCache.addFileToClassPath(new Path(str), conf);
		}
	}

	/**
	 * Returns the size of the file.
	 */
	public static long getFileSize(Path path, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus fileStatus = fs.getFileStatus(path);
		return fileStatus.getLen();
	}

	
	
	public static Path[] getFilesOnly(Path dir, String regex, FileSystem fs) throws FileNotFoundException, IOException{
		//Path qualifiedPath = fs.makeQualified(new Path(dir));
		FileStatus[] status = fs.listStatus(dir);
		ArrayList<Path> arraylist = new ArrayList<Path>();
		Pattern p = Pattern.compile(".*" + regex + ".*");
		for (FileStatus statu : status) {
			String pathStr = statu.getPath().toString();
			if (p.matcher(pathStr).matches()) {
				arraylist.add(statu.getPath());
			}
		}
		arraylist.trimToSize();
		Path[] returnArray = new Path[arraylist.size()];
		returnArray = arraylist.toArray(returnArray);
		return returnArray;
	}
	
	
	public static String[] getFilesAndDirectories(String fileOrDirList,
			boolean recursively, boolean getDirectories, boolean getFiles, Configuration conf)
			throws IOException {
		//Configuration conf = new Configuration();
		String root = conf.get("fs.default.name");

		ArrayList<String> arraylist = new ArrayList<String>();

		Stack<Path> stack = new Stack<Path>();
		String uri = null;

		FileSystem fs1 = null;
		String[] fileOrDir = fileOrDirList.split(",", -1);
		for (String aFileOrDir : fileOrDir) {
			if (aFileOrDir.indexOf(root) == -1) {
				uri = root + aFileOrDir;
			} else {
				uri = aFileOrDir;
			}
			FileSystem fs = FileSystem.get(URI.create(uri), conf);

			Path[] paths = new Path[1];
			paths[0] = new Path(uri);
			FileStatus[] status = fs.listStatus(paths);
			for (FileStatus statu : status) {
				if (statu.isDir()) {
					stack.push(statu.getPath());
					if (getDirectories) {
						arraylist.add(statu.getPath().toString());
					}
				} else {
					if (getFiles) {
						arraylist.add(statu.getPath().toString());
					}
				}
			}

			if (recursively) {
				Path p1 = null;
				FileStatus[] status1 = null;
				while (!stack.empty()) {
					p1 = stack.pop();
					fs1 = FileSystem.get(URI.create(p1.toString()),
							conf);
					paths[0] = new Path(p1.toString());
					status1 = fs1.listStatus(paths);

					for (FileStatus aStatus1 : status1) {
						if (aStatus1.isDir()) {
							stack.push(aStatus1.getPath());
							if (getDirectories) {
								arraylist.add(aStatus1.getPath().toString());
							}
						} else {
							if (getFiles) {
								arraylist.add(aStatus1.getPath().toString());
							}
						}
					}
				}
			}
			fs.close();
		}
		arraylist.trimToSize();
		String[] returnArray = new String[arraylist.size()];

		return arraylist.toArray(returnArray);
	}

	/**
	 * @param fileOrDir
	 *            Comma delimited list of input files or directories in HDFS.
	 *            Input can be given with HDFS URL. i.e.
	 *            "hdfs://hd4.ev1.yellowpages.com:9000/user/directory" and
	 *            "/user/directory" means the same
	 * @param recursively
	 *            When set to "true" then recursively opens all sub directories
	 *            and returns files
	 */
	public static String[] getFilesOnly(String fileOrDir, boolean recursively, Configuration conf)
			throws IOException {
		return getFilesAndDirectories(fileOrDir, recursively, false, true, conf);
	}

	/**
	 * Same as String[] getFilesOnly(String fileOrDir, boolean recursively)
	 * except that it only returns paths that match the regex
	 */
	public static String[] getFilesOnly(String fileOrDir, boolean recursively,
			String regex,  Configuration conf) throws IOException {
		ArrayList<String> arraylist = new ArrayList<String>();
		String[] tempArr = getFilesOnly(fileOrDir, recursively, conf);
		Pattern p = Pattern.compile(".*" + regex + ".*");
		// Extract the file names that match the regex
		for (String aTempArr : tempArr) {
			if (p.matcher(aTempArr).matches()) {
				arraylist.add(aTempArr);
			}
		}
		arraylist.trimToSize();
		String[] returnArray = new String[arraylist.size()];
		returnArray = arraylist.toArray(returnArray);
		return returnArray;
	}

	/**
	 * @param fileOrDir
	 *            Comma delimited list of input files or directories in HDFS.
	 *            Input can be given with HDFS URL. i.e.
	 *            "hdfs://hd4.ev1.yellowpages.com:9000/user/directory" and
	 *            "/user/directory" means the same
	 * @param recursively
	 *            When set to "true" then recursively opens all sub directories
	 *            and returns sub directories
	 */
	public static String[] getDirectoriesOnly(String fileOrDir,
			boolean recursively, Configuration conf) throws IOException {
		return getFilesAndDirectories(fileOrDir, recursively, true, false, conf);
	}

	/**
	 * @param fileOrDir
	 *            Comma delimited list of input files or directories in HDFS.
	 *            Input can be given with HDFS URL. i.e.
	 *            "hdfs://hd4.ev1.yellowpages.com:9000/user/directory" and
	 *            "/user/directory" means the same
	 * @param recursively
	 *            When set to "true" then recursively opens all sub directories
	 *            and returns files and sub directories
	 */
	public static String[] getFilesAndDirectories(String fileOrDir,
			boolean recursively, Configuration conf) throws IOException {
		return getFilesAndDirectories(fileOrDir, recursively, true, true, conf);
	}

	/**
	 * This method uses recursion to retrieve a list of files/directories
	 * 
	 * @param p
	 *            Path to the directory or file you want to start at.
	 * @param configuration
	 *            Configuration
	 * @param files
	 *            a Map<Path,FileStatus> of path names to FileStatus objects.
	 * @throws IOException
	 */
	public static void getFiles(Path p, Map<Path, FileStatus> files,
			Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(p.toUri(), conf);
		if (files == null) {
			files = new HashMap();
		}
		if (fs.isFile(p)) {
			files.put(p, fs.getFileStatus(p));
		} else {
			FileStatus[] statuses = fs.listStatus(p);
			for (FileStatus s : statuses) {
				if (s.isDir()) {
					getFiles(s.getPath(), files, conf);
				} else {
					files.put(s.getPath(), s);
				}
			}
		}
		fs.close();
	}

	/**
	 * This method deletes all zero byte files within a directory and all its
	 * subdirectories
	 * 
	 * @param fileOrDir
	 *            If file then delete the file if its zero bytes, if directory
	 *            then delete all zero bytes files from the directory
	 */
	public static void removeAllZeroByteFiles(String fileOrDir,
			Configuration conf) {
		try {
			Map<Path, FileStatus> files = new HashMap<Path, FileStatus>();
			getFiles(new Path(fileOrDir), files, conf);
			for (Path p : files.keySet()) {
				FileStatus s = files.get(p);
				if (s.getLen() == 0) {
					FileSystem fs = FileSystem.get(p.toUri(), conf);
					fs.delete(p, false);
					fs.close();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method returns the size of file or a directory in HDFS.
	 * 
	 * @param fileOrDir
	 *            file or diretory or list of files or directories in HDFS, if
	 *            directory then size of all files within the directory and its
	 *            subdirectories are returned
	 * @return size of the file or directory (sum of all files in the directory
	 *         and sub directories)
	 */
	public static long size(String fileOrDir, Configuration conf)
			throws IOException {
		long totalSize = 0;
		String allFiles[] = fileOrDir.split(",", -1);

		for (String allFile : allFiles) {
			Path p = new Path(allFile);
			FileSystem fs = FileSystem.get(p.toUri(), conf);
			totalSize = totalSize + fs.getContentSummary(p).getLength();
			fs.close();
		}
		return totalSize;
	}

	/**
	 * The method moves a single or multiple files or directories, if exists, to
	 * trash. It also accepts list of hdfs file or directory delimited by comma.
	 * 
	 * @param fileOrDir
	 *            HDFS file or directory name or list of HDFS file or directory
	 *            names
	 * @throws IOException
	 */

	public static void removeHdfsPath(String fileOrDir, Configuration conf)
			throws IOException {
		FileSystem fs = FileSystem.get(URI.create(fileOrDir), conf);
		String[] fileList = fileOrDir.split(",", -1);
		Trash trash = new Trash(conf);
		trash.expunge();
		for (String aFileList : fileList) {
			Path p = new Path(aFileList);
			if (fs.exists(p)) {
				trash.moveToTrash(p);
			}
		}
		fs.close();
	}

	public static void main(String[] args) {
		try {
			Path destPath = FSUtil.getLocalTempPath();
			
			System.out.println(destPath);
			
			/*Path srcPath = new Path(
					"hdfs://xiliu-fedora:54310/user/cloudetl/output/pagedim");
			FileSystem srcFS = srcPath.getFileSystem(new Configuration());
			FileSystem dstFS = destPath.getFileSystem(new Configuration());

			PathFilter pathFilter = new PathFilter() {
				public boolean accept(Path path) {
					return path.toString().indexOf("map-r") != -1;
				}
			};

			FSUtil.copy(srcFS, srcPath, dstFS, destPath, pathFilter, false, true,
					new Configuration());*/
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
