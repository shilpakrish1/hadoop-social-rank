import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FinishMapper extends Mapper<LongWritable, Text, Text, Text>{
    public void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException {
    	if (value != null) {
    		String[] currLine = value.toString().split("\t");
    		String[] labels = currLine[1].split("!")[0].split(":");
			//Writes the friend recommendation pairs to the reducer for sorting
			for (String s : labels) {
				//Splits into the key, value pairs
				String[] vertices = s.split(",");
				if (vertices.length == 2) {
					context.write(new Text(vertices[0]), new Text(currLine[0] + "," + vertices[1]));
				}
			}	
		}
	}
}

