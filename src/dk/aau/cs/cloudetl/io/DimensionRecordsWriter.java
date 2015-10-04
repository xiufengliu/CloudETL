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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;

import dk.aau.cs.cloudetl.common.CEConstants;
import dk.aau.cs.cloudetl.common.CEException;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.common.FieldType;
import dk.aau.cs.cloudetl.common.Utils;

public class DimensionRecordsWriter extends RecordsWriter {
	List<RecordWritable> records;
	SCDValuesWritable scdValues;
	int iSID = -1;
	int iLookup = -1;
	int iValidFrom = -1;
	int iValidTo = -1;
	int iVer = -1;

	public DimensionRecordsWriter(Context context, DataWriter target) {
		super(context, target);
		this.records = new ArrayList<RecordWritable>();
		this.scdValues = new SCDValuesWritable();
		RecordWritable record = target.getRecord();
		this.iSID = record.indexOf(FieldType.PRI, false);
		this.iLookup = record.indexOf(FieldType.LOOKUP, false);

		if (isSCDWriter) {
			this.iValidFrom = record.indexOf(FieldType.SCD_VALIDFROM, true);
			this.iValidTo = record.indexOf(FieldType.SCD_VALIDTO, true);
			this.iVer = record.indexOf(FieldType.SCD_VERSION, true);
		}

	}

	public void add(RecordWritable destRecord) throws CEException {
		if (isSCDWriter) {
			int size = records.size();
			if (size == 0) {
				Field f = destRecord.getField(iValidFrom);
				if (f.getValue() == null) {
					f.setValue("1900-01-01");
				}
			} else if (size > 0) {
				Field lastValidTo = records.get(size - 1).getField(iValidTo);

				Field lastValidFrom = records.get(size - 1)
						.getField(iValidFrom);

				Field curValidFrom = destRecord.getField(iValidFrom);
				if (curValidFrom.getValue() == null) {
					curValidFrom.setValue(Utils.getTodayAsString());
				}

				if (!Utils.dateAfter(curValidFrom.getValueAsString(),
						lastValidFrom.getValueAsString())) {
					return;
				}

				lastValidTo.setValue(curValidFrom.getValue());
			}

			Field verField = destRecord.getField(iVer);
			verField.setValue(size + 1);

			scdValues.add(new SCDValueWritable((Integer) destRecord.getField(
					this.iSID).getValue(), Utils
					.parseDateToLong((String) destRecord.getField(
							this.iValidFrom).getValue())));
		}
		records.add(destRecord);
	}

	public void writeAll() throws IOException, InterruptedException {
		if (records.size() > 0) {
			if (isSCDWriter) {
				String name = writer.getName();
				Text key = new Text(records.get(0).getField(this.iLookup)
						.getValueAsString());
				mos.getCollector(CEConstants.NAMED_OUTPUT_SCD_MAP, name,
						context).collect(key, scdValues); // Write the lookup index;
				scdValues.clear();
			}

			for (RecordWritable record : records) {
				mos.getCollector(CEConstants.NAMED_OUTPUT_TEXT,
						writer.getName(), context).collect(NullWritable.get(),
						record.toText()); // Write the dimension
			}
			records.clear();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void write(RecordWritable record) throws IOException,
			InterruptedException {
		String name = writer.getName();
		Text key = new Text(record.getField(this.iLookup).getValueAsString());

		IntWritable value = new IntWritable((Integer) record.getField(this.iSID).getValue());

		mos.getCollector(CEConstants.NAMED_OUTPUT_MAP, name, context).collect(key, value); // Write the lookup index;

		mos.getCollector(CEConstants.NAMED_OUTPUT_TEXT, name, context).collect(NullWritable.get(), record.toText()); // Write the dimension data
	}
}
