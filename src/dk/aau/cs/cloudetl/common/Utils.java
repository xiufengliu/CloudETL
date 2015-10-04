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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.text.Collator;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Queue;
import java.util.ResourceBundle;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

public final class Utils {

	// TODO: make thread-local and non-static
	public static final SimpleDateFormat yyyyMMddHHmmssSSS = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss.SSS");
	public static final String RANDOM_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

	public static String LINE_SEPARATOR = System.getProperty("line.separator",
			"\n");

	private Utils() {
	}

	/*
	 * private static Object call(Object o, String methodName, Class[]
	 * parameterTypes, Object[] args) { try { if (o != null) { Class clazz =
	 * o.getClass(); Method method = clazz.getMethod(methodName,
	 * parameterTypes); return method.invoke(o, args); } return null; } catch
	 * (NoSuchMethodException e) { return e; } catch (IllegalAccessException e)
	 * { return e; } catch (Throwable e) { throw DataException.exception(e); } }
	 * 
	 * public static void closeIO(Object o) throws IOException { call(o,
	 * "flush", null, null); call(o, "close", null, null); }
	 */
	public static int read(ReadableByteChannel channel, int amount,
			ByteBuffer dest) throws IOException {
		int read = 0;
		int last = 0;
		if (dest.remaining() < amount) {
			throw new BufferOverflowException();
		}
		while (read < amount && last != -1) {
			last = channel.read(dest);
			if (last != -1)
				read += last;
		}
		return (read == 0 && last == -1) ? -1 : read;
	}

	public static void send(ByteChannel channel, ByteBuffer buf, int seq)
			throws IOException {
		buf.clear();
		buf.putInt(seq);
		buf.flip();
		channel.write(buf);
	}

	public static String readString(ReadableByteChannel channel)
			throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(4);
		return readString(channel, buffer);
	}

	public static String readString(ReadableByteChannel channel,
			ByteBuffer buffer) throws IOException {
		// Get the table name
		buffer.clear();
		read(channel, 4, buffer);
		int l = buffer.getInt(0);
		byte[] tmpArray = new byte[l];
		ByteBuffer tmpBuffer = ByteBuffer.wrap(tmpArray);
		int read = 0;
		while (read < l)
			read += channel.read(tmpBuffer);
		return Utils.getStringFromUtf8(tmpArray);

	}

	public static byte[] getBytesUtf8(String string)
			throws UnsupportedEncodingException {
		if (string == null) {
			return null;
		}
		return string.getBytes("UTF-8");
	}

	public static String getStringFromUtf8(byte[] utf8Bytes) {
		return new String(utf8Bytes, Charset.forName("UTF-8"));
	}

	public static void closeQuietly(ByteChannel channel) {
		try {
			channel.close();
		} catch (Exception e) {
		}
	}

	public static String serializeBase64(Object object) throws IOException {
		return serializeBase64(object, true);
	}

	public static String serializeBase64(Object object, boolean compress)
			throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();

		ObjectOutputStream out = new ObjectOutputStream(
				compress ? new GZIPOutputStream(bytes) : bytes);

		try {
			out.writeObject(object);
		} finally {
			out.close();
		}

		return new String(Base64.encodeBase64(bytes.toByteArray()));
	}

	/**
	 * This method deserializes the Base64 encoded String into an Object
	 * instance.
	 * 
	 * @param string
	 * @return an Object
	 */
	public static Object deserializeBase64(String string) throws IOException {
		return deserializeBase64(string, true);
	}

	public static Object deserializeBase64(String string, boolean decompress)
			throws IOException {
		if (string == null || string.length() == 0)
			return null;

		ObjectInputStream in = null;

		try {
			ByteArrayInputStream bytes = new ByteArrayInputStream(
					Base64.decodeBase64(string.getBytes()));

			in = new ObjectInputStream(decompress ? new GZIPInputStream(bytes)
					: bytes) {
				@Override
				protected Class<?> resolveClass(ObjectStreamClass desc)
						throws IOException, ClassNotFoundException {
					try {
						return Class.forName(desc.getName(), false, Thread
								.currentThread().getContextClassLoader());
					} catch (ClassNotFoundException exception) {
						return super.resolveClass(desc);
					}
				}
			};

			return in.readObject();
		} catch (ClassNotFoundException exception) {
			throw new IOException("unable to deserialize data", exception);
		} finally {
			if (in != null)
				in.close();
		}
	}
	
	 public static boolean dateAfter(String todate, String when, boolean include) {
			try{
				//Calendar cal1 = Calendar.getInstance();
				//Calendar cal2 = Calendar.getInstance();
				SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd"); 
				Date toDate = format.parse(todate);
				Date toBefore = format.parse(when);
				//cal1.setTimeInMillis(toDate.getTime());
				//cal1.setTimeInMillis(toDate.getTime());
				return include?toDate.getTime()>=toBefore.getTime():toDate.getTime()>toBefore.getTime();
			} catch (ParseException e){
				e.printStackTrace();
			}
			return false;
	 }
	 
	 public static boolean dateAfter(String todate, String when) {
			return Utils.dateAfter(todate, when, false);
	 }
	 
	public static boolean dateBefore(String dateStr, String when) {
		try {
			if (Utils.isEmptyORNull(when)) {
				return Utils.isEmptyORNull(dateStr) ? false : true;
			} else {
				if (Utils.isEmptyORNull(dateStr)) {
					return false;
				} else {
					SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
					Date theDate = format.parse(dateStr);
					Date target = format.parse(when);
					return theDate.getTime() < target.getTime();
				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static long parseDateToLong(String  dateStr) throws CEException{
		return parseDateToLong(dateStr, "yyyy-MM-dd");
	}
	
	public static long parseDateToLong(String  dateStr, String pattern) throws CEException{
	        final SimpleDateFormat sdf = new SimpleDateFormat(pattern);
	        try
	        {
	            Date d = sdf.parse(dateStr);
	            return d.getTime();
	        } 
	        catch (ParseException e)
	        {
	            throw new CEException(e);
	        }
	}
	
	
	public static String getCurrentTimeStamp() {
	    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
	    return sdf.format(new Date());
	}
	
	public static long getCurrentTime() {
	    Date now = new Date();
	    return now.getTime();
	}
	
	
	  public static Object invokeStaticMethod( Class type, String methodName, Object[] parameters, Class[] parameterTypes ) throws CEException
	    {
	    try
	      {
	      Method method = type.getDeclaredMethod( methodName, parameterTypes );

	      method.setAccessible( true );

	      return method.invoke( null, parameters );
	      }
	    catch( Exception exception )
	      {
	      throw new CEException( "unable to invoke static method: " + type.getName() + "." + methodName, exception );
	      }
	    }
	
	
	
	  public static String normalizeUrl( String url )
	    {
	    if( url == null )
	      return null;

	    return url.replaceAll( "([^:]/)/{2,}", "$1/" );
	    }

	  

	public static void closeIO(Closeable o) throws IOException {
		if (o instanceof Flushable) {
			Flushable f = (Flushable) o;
			f.flush();
		}
		o.close();
	}

	public static final String toPrintableString(int c) {
		// TODO: expand with more non-printable characters
		switch (c) {
		case -1:
			return "end-of-file";
		case '\n':
			return "end-of-line (\\n)";
		case '\r':
			return "end-of-line (\\r)";
		case '\t':
			return "tab (\\t)";
		case ' ':
			return "space";
		case 0:
			return "null char (0)";
		default:
			return String.valueOf((char) c);
		}
	}

	public static String getRandomString(int length) {
		StringBuffer s = new StringBuffer(length);
		int randomCharsLength = RANDOM_CHARS.length();
		for (int i = 0; i < length; i++) {
			s.append(RANDOM_CHARS.charAt((int) (randomCharsLength * Math
					.random())));
		}
		return s.toString();
	}

	public static String intToColumnName(int column) {
		// log.debug("intToColumnName("+column+")");
		StringBuffer s = new StringBuffer();

		column++;

		while (column > 26) {
			int c = column % 26;
			column = column / 26;

			if (c == 0) {
				s.insert(0, 'Z');
				column--;
			} else {
				s.insert(0, (char) (c + 'A' - 1));
			}
		}
		s.insert(0, (char) (column + 'A' - 1));

		// log.debug("    " + s + "  =>  " + columnNameToInt(s.toString()));
		return s.toString();
	}

	public static int columnNameToInt(String columnName) {
		int length = columnName.length();
		int column = 0;

		for (int i = 0; i < length; i++) {
			char c = Character.toUpperCase(columnName.charAt(i));
			if (i < length - 1) {
				column += Math.pow(26, length - i - 1) * (c - 'A' + 1);
			} else {
				column += c - 'A';
			}
		}

		return column;
	}

	public static boolean namesMatch(String name1, String name2) {
		if (name1 == name2) {
			return true;
		} else if (name1 == null || name2 == null) {
			return false;
		}
		return name1.equalsIgnoreCase(name2);
	}

	public static boolean isWhitespace(int c) {
		return c == ' ' || c == '\t';
		// return Character.isWhitespace(c);
	}

	public static boolean isWhitespace(int c, int exception) {
		if (c == exception) {
			return false;
		}
		return (c == ' ' || c == '\t');
	}

	// public static boolean isWhitespace(int c, int exception, int ...
	// exceptions) {
	// if (c == exception) {
	// return false;
	// }
	// if (exceptions != null) {
	// for (int e : exceptions) {
	// if (c == e) {
	// return false;
	// }
	// }
	// }
	// return (c == ' ' || c == '\t');
	// }

	public static String getTypeName(Object value) {
		if (value == null) {
			return "";
		}
		String type = value.getClass().getName();
		int index = type.lastIndexOf('.');
		type = type.substring(index + 1);
		return type;
	}

	public static boolean isEmpty(String s) {
		return s == null || s.trim().length() == 0;
	}
	

	public static boolean isEmptyORNull(String s) {
		return s == null || s.trim().length() == 0 || "null".equals(s);
	}
	
	public static boolean isNotEmpty(String s) {
		return !isEmpty(s);
	}

	public static int compare(Object o1, Object o2) {
		return compare(o1, o2, null);
	}

	public static <T> int compare(T o1, T o2, Collator collator) {
		if (o1 == o2) {
			return 0;
		} else if (o1 == null) {
			return -1;
		} else if (o2 == null) {
			return 1;
		} else if (collator != null) {
			return collator.compare(o1, o2);
		} else {
			return ((Comparable<T>) o1).compareTo(o2);
		}
	}

	public static boolean equals(Object o1, Object o2) {
		return equals(o1, o2, null);
	}

	public static boolean equals(Object o1, Object o2, Collator collator) {
		if (o1 == o2) {
			return true;
		} else if (o1 == null || o2 == null) {
			return false;
		} else if (collator != null) {
			return collator.compare(o1, o2) == 0;
		} else {
			return o1.equals(o2);
		}
	}

	public static int compare(String s1, String s2, boolean caseSensitive) {
		if (s1 == s2) {
			return 0;
		} else if (s1 == null) {
			return -1;
		} else if (s2 == null) {
			return 1;
		} else {
			return caseSensitive ? s1.compareTo(s2) : s1
					.compareToIgnoreCase(s2);
		}
	}

	public static boolean matches(String s1, String s2, boolean caseSensitive,
			boolean allowEmpty) {
		if (!allowEmpty && (isEmpty(s1) || isEmpty(s2))) {
			return false;
		}
		return compare(s1, s2, caseSensitive) == 0;
	}

	public static boolean matches(String s1, String s2, boolean caseSensitive) {
		return compare(s1, s2, caseSensitive) == 0;
	}

	public static boolean matches(String s1, String s2) {
		return matches(s1, s2, true);
	}

	public static String arrayToString(Object[] array, String separator) {
		if (array == null || array.length == 0) {
			return "";
		}

		StringBuffer s = new StringBuffer(32 * array.length);

		s.append(array[0]);

		for (int i = 1; i < array.length; i++) {
			s.append(separator).append(array[i]);
		}

		return s.toString();
	}

	public static String arrayToString(List<?> array, String separator) {
		if (array == null || array.size() == 0) {
			return "";
		}

		StringBuffer s = new StringBuffer(32 * array.size());

		s.append(array.get(0));
		for (int i = 1; i < array.size(); i++) {
			s.append(separator).append(array.get(i));
		}

		return s.toString();
	}

	public static String arrayToString(int[] array, String separator) {
		if (array == null || array.length == 0) {
			return "";
		}

		StringBuffer s = new StringBuffer(32 * array.length);

		s.append(array[0]);

		for (int i = 1; i < array.length; i++) {
			s.append(separator).append(array[i]);
		}

		return s.toString();
	}

	public static String arrayToString(double[] array, String separator) {
		if (array == null || array.length == 0) {
			return "";
		}

		StringBuffer s = new StringBuffer(32 * array.length);

		s.append(array[0]);

		for (int i = 1; i < array.length; i++) {
			s.append(separator).append(array[i]);
		}

		return s.toString();
	}

	public static String collectionToString(Collection<?> collection,
			String separator) {
		if (collection == null || collection.size() == 0) {
			return "";
		}

		StringBuffer s = new StringBuffer(32 * collection.size());

		Iterator<?> i = collection.iterator();
		s.append(i.next());

		while (i.hasNext()) {
			s.append(separator).append(i.next());
		}

		return s.toString();
	}

	/**
	 * Returns the number of <code>searchChar</code>s contained in
	 * <code>string</code> or zero (0).
	 */
	public static int getCharacterCount(String string, char searchChar) {
		int index = 0;
		int count = 0;

		while ((index = string.indexOf(searchChar, index)) >= 0) {
			count++;
			index++;
		}

		return count;
	}

	/**
	 * Returns the first key encountered in a map for the specified value or
	 * <CODE>null</CODE>.
	 * 
	 */
	public static <K, V> K getKeyForValue(V value, Map<K, V> map) {
		for (Iterator<Map.Entry<K, V>> i = map.entrySet().iterator(); i
				.hasNext();) {
			Map.Entry<K, V> entry = i.next();
			if (value.equals(entry.getValue())) {
				return entry.getKey();
			}
		}
		return null;
	}

	private static StringBuffer pad(StringBuffer string, int size,
			char padChar, boolean left) {
		if (size < 0) {
			size = 0;
		}

		string.ensureCapacity(size);
		while (string.length() < size) {
			if (left) {
				string.insert(0, padChar);
			} else {
				string.append(padChar);
			}
		}

		return string;
	}

	public static String padLeft(String string, int size, char padChar) {
		return pad(new StringBuffer(string), size, padChar, true).toString();
	}

	public static String padRight(String string, int size, char padChar) {
		return pad(new StringBuffer(string), size, padChar, false).toString();
	}

	public static String repeat(String string, int count) {
		if (count < 0) {
			count = 0;
		}

		StringBuffer s = new StringBuffer(string.length() * count);

		for (int i = 0; i < count; i++) {
			s.append(string);
		}

		return s.toString();
	}

	public static Object[] resizeArray(Object[] array, int difference) {
		if (difference == 0) {
			return array;
		} else {
			int newSize = array.length + difference;
			Object[] newArray = (Object[]) Array.newInstance(array.getClass()
					.getComponentType(), newSize);

			if (difference > 0) {
				System.arraycopy(array, 0, newArray, 0, array.length); // grow
			} else {
				System.arraycopy(array, 0, newArray, 0, newSize); // shrink
			}

			return newArray;
		}
	}

	public static int[] resizeArray(int[] array, int difference) {
		if (difference == 0) {
			return array;
		} else {
			int newSize = array.length + difference;
			int[] newArray = (int[]) Array.newInstance(array.getClass()
					.getComponentType(), newSize);

			if (difference > 0) {
				System.arraycopy(array, 0, newArray, 0, array.length); // grow
			} else {
				System.arraycopy(array, 0, newArray, 0, newSize); // shrink
			}

			return newArray;
		}
	}


	
	 public static String[] split(String str, String delims, boolean trimTokens) {
		    StringTokenizer tokenizer = new StringTokenizer(str, delims);
		    int n = tokenizer.countTokens();
		    String[] list = new String[n];
		    for (int i = 0; i < n; i++) {
		      if (trimTokens) {
		        list[i] = tokenizer.nextToken().trim();
		      } else {
		        list[i] = tokenizer.nextToken();
		      }
		    }
		    return list;
		  }

	
	 public static String arrayToString(String[] stringArray, String OutputDelimiter) {
	        int linenum = 0;
	        StringBuffer sb = new StringBuffer();
	        for (int i = 0; i < stringArray.length; i++) {
	            linenum++;
	            if (linenum == 1) {
	                sb.append(stringArray[i]);
	            } else {
	                sb.append(OutputDelimiter);
	                sb.append(stringArray[i]);
	            }
	        }
	        return sb.toString();
	    }
	 
	 
	 @SuppressWarnings("unchecked")
	    public static List split(String target, String delimiter) { // Split the csv line.
	        List result = new ArrayList();
	        int pos = 0;
	        String temp = target;
	        while ((pos = temp.indexOf(delimiter)) != -1) {
	            result.add(temp.substring(0, pos));
	            temp = temp.substring(pos + 1);
	        }
	        if (target.endsWith(delimiter)) {
	            result.add(null);
	        } else {
	            result.add(temp);
	        }
	        return result;
	    }

	    @SuppressWarnings("unchecked")
	    public static List<String> split(String str) {
	        List<String> list = new ArrayList<String>();

	        // .*".+".*
	        String pattern = ".*\".+\".*";
	        Pattern p = Pattern.compile(pattern);
	        Matcher matcher = p.matcher(str);

	        boolean special = matcher.find();

	        if (special) {
	            // sign stack
	            Stack<String> signStack = new Stack<String>();
	            // String queue
	            Queue<String> stringQueue = new LinkedList<String>();

	            boolean closed = true;

	            // remove "/r", "/n"
	            str = str.replace("\r", "").replace("\n", "");

	            int length = str.length();

	            StringBuilder string = new StringBuilder();
	            for (int i = 0; i < length; i++) {
	                char c = str.charAt(i);

	                if (closed && ',' == c) {
	                    // ","
	                    // enqueue to the string
	                    stringQueue.offer(string.toString());
	                    // reset the string
	                    string = new StringBuilder();
	                } else if ('\"' == c) {
	                    // """
	                    if (closed) {
	                        // set the close flag
	                        closed = false;
	                        // push to the stack
	                        signStack.push("#");
	                        // do nothing
	                    } else {
	                        // unclosed
	                        if (length == i + 1 || ',' == str.charAt(i + 1)) {
	                            // last character or following a ","
	                            // reset the close flag
	                            closed = true;
	                            // pop from the stack
	                            signStack.pop();
	                            // do nothing
	                        } else {
	                            // other condition
	                            // ignore the """
	                            // add the next character to the string, and skip it
	                            string.append(str.charAt(++i));
	                            // do nothing with the flag and the stack
	                        }
	                    }
	                } else {
	                    // add to the string
	                    string.append(c);
	                }

	            } // end for

	            // last string is left, add into queue
	            stringQueue.add(string.toString());

	            // analyze fail
	            if (!closed || !signStack.isEmpty()) {
	                return null;
	            } else {
	                // insert into the return list
	                list.addAll(stringQueue);
	            }
	        } else {
	            list = Utils.split(str, ",");
	        }

	        return list;
	    }
	 

	    /**
	     * This method takes an input string and returns the default value if the input string is null.
	     *
	     * @param input The string that needs to be checked for null
	     * @defaultValueIfInputIsNull If the "input" is null then replace it by this default value.
	     */

	    public static String ifNull(String input, String defaultValueIfInputIsNull) {
	        String output = null;

	        if (input == null) {
	            output = defaultValueIfInputIsNull;
	        } else {
	            output = input;
	        }
	        return output;


	    }

	    /**
	     * This method splits a string by the delimiter and returns the value at the given position
	     *
	     * @param str         The input string
	     * @param delimiter   The delimiter that separates different values of the string
	     * @param tokenNumber The value of the string at the given tokenNumber. It works with 0 index.
	     * @return Returns null if the tokenNumber is greater than the total number of tokens in the string
	     *         <p/>
	     *         <b>Example:</b> For string "a:b:c:d:e:f:g", if the method is run with delimiter=":" and tokenNumber=1, it
	     *         will return "b"
	     */
	    public static String getValueAtToken(String str, String delimiter, int tokenNumber) {
	        String[] tokens = str.split(delimiter, -1);
	        String returnString = null;
	        for (int i = 0; i < tokens.length; i++) {
	            if (tokenNumber == i) {
	                returnString = tokens[i];
	            }
	        }
	        return returnString;
	    }

	    /**
	     * This method merges 2 input string arrays and returns the merged array as output
	     *
	     * @param inputArray1 Input Array 1 that needs to be merged
	     * @param inputArray2 Input Array 2 that needs to be merged
	     * @return Returns merged Array that contains all elements of inputArray1 followed by elements of inputArray2
	     *         <p/>
	     *         <b>Example:</b> The following are the input array
	     *         <p/>
	     *         inputArray1 contains elements ("a","b","c")
	     *         <p/>
	     *         inputArray2 contains element ("b","e")
	     *         <p/>
	     *         then the output array contains elements
	     *         <p/>
	     *         ("a","b","c","b","e")
	     */
	    public static String[] mergeArrays(String[] inputArray1, String[] inputArray2) {
	        String[] outputArray = new String[inputArray1.length + inputArray2.length];
	        /**
	         * Copy first array to outputArray
	         */
	        for (int i = 0; i < inputArray1.length; i++) {
	            outputArray[i] = inputArray1[i];
	        }

	        /**
	         * Copy second array to outputArray
	         */
	        int outputArrayLength = inputArray1.length;
	        for (int i = 0; i < inputArray2.length; i++) {
	            outputArray[i + outputArrayLength] = inputArray2[i];
	        }

	        return outputArray;
	    }


	    /**
	     * Get property value from resource bundle
	     *
	     * @param rb           ResourceBundle instance
	     * @param propertyName property name
	     * @return property value
	     */
	    public static String getPropertyValue(ResourceBundle rb, String propertyName) {
	        String propertyValue = null;
	        try {
	            propertyValue = rb.getString(propertyName);
	        } catch (MissingResourceException ex) {
	            System.err.println("Could not find value for property: " + propertyName);
	        }
	        return propertyValue;
	    }

	    /**
	     * This method parses a string and return true if its an integer else it return false
	     *
	     * @param input
	     * @return
	     */
	    public static boolean isInteger(String input) {
	        try {
	            Integer.parseInt(input);
	            return true;
	        } catch (Exception e) {
	            return false;
	        }
	    }
	    
	    public static String getTodayAsString() {
	        Calendar calendar = Calendar.getInstance();
	        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	        return dateFormat.format(calendar.getTime());
	    }

	    
	    public static void main(String[]args){
	    	String str = "70	SomeServer/1.0	Test1			1999-12-14	";
	    	List<String> lists = Utils.split(str, "\t");
	    	for (String l: lists){
	    		System.out.println("c="+l);
	    	}
	    }

	

}
