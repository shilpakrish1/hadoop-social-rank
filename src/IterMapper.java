import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//Parses the current string
		String[] currLine = value.toString().split("\t");
		String[] values = currLine[1].split("!");
		String[] labels = values[0].split(":");
		String[] adjacencyList = values[1].split(":");
		double dampingFactor = .85;
		//Propagates the label weights to the edges
		for (int i = 0; i < labels.length; i++) {
			for (int j = 0; j < adjacencyList.length; j++) {
				String[] currLabel = labels[i].split(",");
				String[] edge = adjacencyList[j].split(",");
				if (currLabel.length == 2 && edge.length == 2) {
					double prop = dampingFactor * Double.parseDouble(currLabel[1]) * Double.parseDouble(edge[1]);
					double roundedProp =  (double) Math.round(prop * 1000) / 1000;
					context.write(new Text(edge[0]), new Text(currLabel[0] + "," + Double.toString(roundedProp)));
				}
			}
		}
	//Propagates the adjacency list to the reducer
	context.write(new Text(currLine[0]), new Text("adjList" + "@" + currLine[1]));
	}
}