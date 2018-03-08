import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class InitReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
		//StringBuilder for adjacency list
		StringBuilder adjList = new StringBuilder();
		int totalAffil = 0;
		//Creates new HashMap containing outgoing edges and their weight
		HashMap<String, Integer> edgeMap = new HashMap<String,Integer>();
		for (Text curr : values) {
			String currEdge = curr.toString();
			Integer currValue = edgeMap.get(currEdge);
			//Puts the current value in the HashMap, incrementing its weight of it already exists
			edgeMap.put(currEdge, (currValue==null) ? 1 : currValue++);
			totalAffil++;
		}
		//Appends outgoing edge, weight pairs
		for (Entry<String, Integer> e : edgeMap.entrySet()) {
			double proportion = (double) e.getValue() / totalAffil;
			double roundedProp = (double) (Math.round(proportion * 1000)) / 1000;
			adjList.append(e.getKey() + "," + roundedProp + ":");
		}
	  //Writes the key and label pairs
	  context.write(new Text(key), new Text(key + "," + "1!" + adjList.toString()));
	}
}
	


