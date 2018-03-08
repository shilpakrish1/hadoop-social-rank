import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException  {
		String currLine = value.toString();
		//Splits the current line
		String[] split = currLine.split("\t");
		if (split.length >= 2) {
			String[] groupMembers = split[1].split(",");
			//Adds edges for those that are in the same group or share the same interest
			if(split[0].startsWith("G") || split[0].startsWith("I")) {
				for (int i = 0; i < groupMembers.length; i++) {
					for (int j = 0; j < groupMembers.length; j++) {
						if (i != j) {
							context.write(new Text(groupMembers[i]), new Text(groupMembers[j]));
						}
					}
				}	
			}
			else {
				//Adds edges unidirectionally for friends
				for (int i = 0; i < groupMembers.length; i++) {
					context.write(new Text(split[0]), new Text(groupMembers[i]));
				}
			}
		}
	}
}


