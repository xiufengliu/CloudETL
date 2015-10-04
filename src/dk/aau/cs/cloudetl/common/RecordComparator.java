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
import java.util.ArrayList;
import java.util.Comparator;

import dk.aau.cs.cloudetl.io.RecordWritable;

public class RecordComparator implements Comparator<RecordWritable>, Cloneable {
    
    static final RecordComparator DEFAULT = new RecordComparator() {

        public RecordComparator add(String fieldName, boolean ascending, Collator collator) {
            throw new UnsupportedOperationException("Immutable instance");
        }

        public RecordComparator add(String fieldName, boolean ascending) {
            throw new UnsupportedOperationException("Immutable instance");
        }

        public RecordComparator add(String fieldName) {
            throw new UnsupportedOperationException("Immutable instance");
        }

    };
    
    //==============================================================

    private final ArrayList<FieldComparator> fieldComparators = new ArrayList<FieldComparator>();
    protected boolean cacheFieldIndexes;  // modified by subclass
    
    public RecordComparator() {
    }
    
    public RecordComparator(RecordComparator o) {
        for (int i = 0; i < o.getCount(); i++) {
            fieldComparators.add((FieldComparator) o.get(i).clone());
        }
        cacheFieldIndexes = o.cacheFieldIndexes;
    }
    
    public Object clone() {
        try {
            return super.clone();
        } catch (CloneNotSupportedException e) {
        	e.printStackTrace();
           return null;
			
        }
    }
    
    
    public RecordComparator add(String fieldName) {
        fieldComparators.add(new FieldComparator()
            .setFieldName(fieldName));
        return this;
    }
    
    public RecordComparator add(String fieldName, boolean ascending) {
        fieldComparators.add(new FieldComparator()
            .setFieldName(fieldName)
            .setAscending(ascending));
        return this;
    }
    
    public RecordComparator add(String fieldName, boolean ascending, Collator collator) {
        fieldComparators.add(new FieldComparator()
            .setFieldName(fieldName)
            .setAscending(ascending)
            .setCollator(collator));
        return this;
    }
    
    public int getCount() {
        return fieldComparators.size();
    }
    
    public FieldComparator get(int index) {
        return (FieldComparator) fieldComparators.get(index);
    }
    
    public RecordComparator remove(int index) {
        fieldComparators.remove(index);
        return this;
    }
    
    public RecordComparator removeAll() {
        fieldComparators.clear();
        return this;
    }
    
    public int compare(RecordWritable record1, RecordWritable record2) {
        if (record1 == record2) {
            return 0;
        } else if (record1 == null) {
            return -1;
        } else if (record2 == null) {
            return 1;
        }

        if (getCount() > 0) {   // fields have been explicitly selected for comparison
            for (int i = 0; i < getCount(); i++) {
                int difference = get(i).compareRecord(record1, record2, cacheFieldIndexes);
                if (difference != 0) {
                    return difference;
                }
            }
        } else {  // no fields specified -- compare all fields
            
            // check record1's fields against record2
            for (int i = 0; i < record1.getFieldCount(); i++) {
                Field field1 = record1.getField(i);
                String field1Name = field1.getName();
                
                int field2Index = record2.indexOf(field1Name, false);
                if (field2Index < 0) { // record1 contains a field record2 doesn't
                    return 1;
                }
                
                Field field2 = record2.getField(field2Index);

                int difference = FieldComparator.DEFAULT.compare(field1, field2);
                if (difference != 0) {
                    return difference;
                }
            }
            
            // record2 contains fields that aren't in record1 
            if (record1.getFieldCount() < record2.getFieldCount()) {
                return -1;
            }

        }
        
        return 0;
    }

}