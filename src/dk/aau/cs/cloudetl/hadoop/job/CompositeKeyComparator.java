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

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import dk.aau.cs.cloudetl.io.SorrogateKeyWritable;


public class CompositeKeyComparator extends WritableComparator {

	protected CompositeKeyComparator() {
		super(SorrogateKeyWritable.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		SorrogateKeyWritable ip1 = (SorrogateKeyWritable) w1;
		SorrogateKeyWritable ip2 = (SorrogateKeyWritable) w2;

		int cmp = ((String)ip1.getBKey()).compareTo((String)ip2.getBKey());
		if (cmp != 0) {
			return cmp;
		}

		return ip1.getSID() == ip2.getSID() ? 0 : (ip1
				.getSID() < ip2.getSID() ? -1 : 1);

	}

}