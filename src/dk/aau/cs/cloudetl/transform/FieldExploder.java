package dk.aau.cs.cloudetl.transform;

import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import dk.aau.cs.cloudetl.common.DataType;
import dk.aau.cs.cloudetl.common.Field;
import dk.aau.cs.cloudetl.io.RecordWritable;

public class FieldExploder extends Transformer implements Serializable {
	 private final String name;
	 private String regex;
	 private String[]dstFieldNames;
	 
	public FieldExploder(String name, String regex, String[]dstFieldNames){
		this.name = name;
		this.regex = regex;
		this.dstFieldNames = dstFieldNames;
	}
	
	@Override
	public void setup(TaskAttemptContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void cleanup(TaskAttemptContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean transform(RecordWritable record) throws Throwable {
		Field field = record.getField(name, true);
		String value = field.getValueAsString();
		Pattern pattern = Pattern.compile(this.regex);
		Matcher m = pattern.matcher(value);
		
		if (m.groupCount() == dstFieldNames.length) {
			for (int i=0; i<this.dstFieldNames.length; ++i) {
				Field f = new Field(dstFieldNames[i],  DataType.STRING);
				f.setValue(m.group(i));
				record.addField();
			}
			return true;
		}
		return false;
	}

}
