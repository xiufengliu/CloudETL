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
package dk.aau.cs.cloudetl.prepartition;

public interface PartConfigKeys {

	static final String OUTPUT_FILENAME = "cloudetl.prepart.output.name";
	static final String PARTITION_NUM = "cloudetl.prepart.number";
	static final String FIELD_DELIM = "cloudetl.prepart.field.delim";
	static final String KEY_INDEX = "cloudetl.prepart.key.index";
	
	static final String COL_LOCATED_FILENAME_PATTERN = "cloudetl.prepart.colocated.filename.pattern";
	static final String IMPORT_MAP_TASKS_NUM = "cloudetl.import.map.tasks.num";
}
