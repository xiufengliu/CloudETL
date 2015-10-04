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

import java.text.Collator;
import java.util.Comparator;

import dk.aau.cs.cloudetl.io.RecordWritable;

public class FieldComparator implements Comparator<Field>, Cloneable {

	static final FieldComparator DEFAULT = new FieldComparator() {
		public FieldComparator setAscending(boolean ascending) {
			throw new UnsupportedOperationException("Immutable instance");
		}

		public FieldComparator setFieldName(String fieldName) {
			throw new UnsupportedOperationException("Immutable instance");
		}

		public int compareRecord(RecordWritable record1, RecordWritable record2,
				boolean cacheFieldIndexes) {
			throw new UnsupportedOperationException("Immutable instance");
		}

	};

	private String fieldName;
	private int fieldIndex = -1;
	private boolean ascending = true;
	private Collator collator;

	public FieldComparator() {
	}

	public FieldComparator copyFrom(FieldComparator o) {
		fieldName = o.fieldName;
		fieldIndex = o.fieldIndex;
		ascending = o.ascending;
		collator = o.collator;
		return this;
	}

	public boolean isAscending() {
		return ascending;
	}

	public FieldComparator setAscending(boolean ascending) {
		this.ascending = ascending;
		return this;
	}

	public Collator getCollator() {
		return collator;
	}

	public FieldComparator setCollator(Collator collator) {
		this.collator = collator;
		return this;
	}

	public String getFieldName() {
		return fieldName;
	}

	public FieldComparator setFieldName(String fieldName) {
		this.fieldName = fieldName;
		return this;
	}

	public Object clone() {
		try {
			return super.clone();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
			throw null;
		}
	}

	private int applySortDirection(int difference) {
		return difference * (ascending ? 1 : -1);
	}

	public int compare(Field o1, Field o2) {
		if (o1 == o2) {
			return 0;
		} else if (o1 == null) {
			return applySortDirection(-1);
		} else if (o2 == null) {
			return applySortDirection(1);
		}

		return applySortDirection(o1.compareTo(o2, collator));
	}

	public int compareRecord(RecordWritable record1, RecordWritable record2, boolean cacheFieldIndexes) {
		Field field1;
		Field field2;

		if (cacheFieldIndexes) {
			if (fieldIndex < 0) {
				fieldIndex = record1.indexOf(fieldName, true);
			}

			field1 = record1.getField(fieldIndex);
			field2 = record2.getField(fieldIndex);
		} else {
			field1 = record1.getField(fieldName);
			field2 = record2.getField(fieldName);
		}

		return compare(field1, field2);
	}

}