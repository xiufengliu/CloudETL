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

package dk.aau.cs.cloudetl.io;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;

public abstract class DataWriter implements Serializable {


	private static final long serialVersionUID = -533846806994202603L;

	String outputDir;
	String name;
	RecordWritable record;

	public DataWriter(String outputDir, String name) {
		this.outputDir = outputDir;
		this.name = name;
		this.record = new RecordWritable();
	}

	public DataWriter setField(String fieldName, DataType dataType) {
		return setField(fieldName, dataType, FieldType.OTHER);
	}

	public DataWriter setField(String fieldName, DataType dataType,
			FieldType fieldType) {
		record.addField(new Field(fieldName, dataType, fieldType));
		return this;
	}

	public List<Field> getFields() {
		return this.record.getFields();
	}

	public Field getField(FieldType fieldType){
		List<Field> fields = record.getFields();
		for (Field field : fields){
			if (field.getFieldType()==fieldType){
				return field;
			}
		}
		return null;
	}
	
	public RecordWritable getRecord() {
		return record;
	}

	public DataWriter setFields(List<Field> list) {
		for (Field field : list)
			this.record.addField(field);
		return this;
	}

	public String getName() {
		return this.name;
	}

	public String getOutputDir(){
		return this.outputDir;
	}

	
	public DataReader asReader(Configuration conf, Path inputDir) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		Path qualifiedPath = new Path(inputDir, name).makeQualified(fs);
		DataReader reader = new CSVFileReader(qualifiedPath.toString());
		reader.setFields(record.getFields());
		return reader;
	}
}
