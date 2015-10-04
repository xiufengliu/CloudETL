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


public enum DataType {

    UNDEFINED(4) {
        public int getSizeInBytes(Object value) {
            return STRING.getSizeInBytes(value);
        }
    },
    
    STRING(4) {
        public int getSizeInBytes(Object value) {
            if (value == null) {
                return 0;
            }
            return super.getSizeInBytes(value) + (value.toString().length() * 2); // length + chars
        }
    },
    
    INT(4),
    LONG(8),
    DOUBLE(8),
    DATETIME(8),
    BOOLEAN(1),
    BYTE(1),
    FLOAT(4),
    SHORT(2),
    CHAR(2),
    DATE(8),
    TIME(8),
    
    BLOB(0) {
        public int getSizeInBytes(Object value) {
            if (value == null) {
                return 0;
            }
            return ((byte[])value).length;
        }
    };
    
    private final int sizeInBytes;
    
    private DataType(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }
    
    public int getSizeInBytes(Object value) {
        return sizeInBytes;
    }
    
}