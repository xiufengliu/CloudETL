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

import java.util.HashMap;

public final class DataTypeUtil {

	private DataTypeUtil() {
	}

	private static final HashMap<DataType, Class<?>> TYPE_TO_CLASS = new HashMap<DataType, Class<?>>();
	static {
		TYPE_TO_CLASS.put(DataType.STRING, String.class);
		TYPE_TO_CLASS.put(DataType.DATE, String.class);
		TYPE_TO_CLASS.put(DataType.DATETIME, String.class);
		TYPE_TO_CLASS.put(DataType.INT, Integer.TYPE);
		TYPE_TO_CLASS.put(DataType.LONG, Long.TYPE);
		TYPE_TO_CLASS.put(DataType.DOUBLE, Double.TYPE);
		TYPE_TO_CLASS.put(DataType.BOOLEAN, Boolean.TYPE);
		TYPE_TO_CLASS.put(DataType.BYTE, Byte.TYPE);
		TYPE_TO_CLASS.put(DataType.FLOAT, Float.TYPE);
		TYPE_TO_CLASS.put(DataType.SHORT, Short.TYPE);
		TYPE_TO_CLASS.put(DataType.CHAR, Character.TYPE);
	}

	

	
	public static Object toJavaType(Object value, DataType dataType)
			throws CEException {
		Class<?> type = TYPE_TO_CLASS.get(dataType);

		if (value==null||"".equals(value))
			return null;
		
		if (value != null && type == value.getClass())
			return value;

		if (type == Object.class)
			return value;

		if (type == String.class)
			return toString(value);

		if (type == Integer.class || type == int.class)
			return toInteger(value);

		if (type == Long.class || type == long.class)
			return toLong(value);

		if (type == Double.class || type == double.class)
			return toDouble(value);

		if (type == Float.class || type == float.class)
			return toFloat(value);

		if (type == Short.class || type == short.class)
			return toShort(value);

		if (type == Boolean.class || type == boolean.class)
			return toBoolean(value);

		if (type != null)
			throw new CEException("could not coerce value, " + value
					+ " to type: " + type.getName());

		return null;
	}

	public static final String toString(Object value) {
		if (value == null)
			return null;

		return value.toString();
	}

	public static final int toInteger(Object value) {
		if (value instanceof Number)
			return ((Number) value).intValue();
		else if (value == null)
			return 0;
		else
			return Integer.parseInt(value.toString());
	}

	public static final long toLong(Object value) {
		if (value instanceof Number)
			return ((Number) value).longValue();
		else if (value == null)
			return 0;
		else
			return Long.parseLong(value.toString());
	}

	public static final double toDouble(Object value) {
		if (value instanceof Number)
			return ((Number) value).doubleValue();
		else if (value == null)
			return 0;
		else
			return Double.parseDouble(value.toString());
	}

	public static final float toFloat(Object value) {
		if (value instanceof Number)
			return ((Number) value).floatValue();
		else if (value == null)
			return 0;
		else
			return Float.parseFloat(value.toString());
	}

	public static final short toShort(Object value) {
		if (value instanceof Number)
			return ((Number) value).shortValue();
		else if (value == null)
			return 0;
		else
			return Short.parseShort(value.toString());
	}

	public static final boolean toBoolean(Object value) {
		if (value instanceof Boolean)
			return ((Boolean) value).booleanValue();
		else if (value == null)
			return false;
		else
			return Boolean.parseBoolean(value.toString());
	}

	public static Class<?> getJavaClass(DataType dataType){
		return TYPE_TO_CLASS.get(dataType);
	}
}
