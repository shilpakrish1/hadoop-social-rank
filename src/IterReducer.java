import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) 
					   throws IOException, InterruptedException {
		StringBuilder labels = new StringBuilder();
		StringBuilder adjList = new StringBuilder();
		//HashMap to store the existing labels (represents vertex, probability pairs)
		HashMap<String, String> vertices = new HashMap<String, String>();
		ArrayList<String> valuesList = new ArrayList<String>();
		for (Text curr : values) {
			valuesList.add(curr.toString());
			String currString = curr.toString();
			//Appends the values from the previous iteration
			if (currString.startsWith("adjList")) {
				//Parses the data and adds the adjacency list to the StringBuilder
				String mapperValues = currString.split("@")[1];
				String[] data = mapperValues.split("!");
				labels.append(data[0] + ":");
				adjList.append(data[1]);
				String[] currVertices = data[0].split(":");
				for (String s : currVertices) {
					String[] vertex = s.split(",");
					if (vertex.length == 2) {
						vertices.put(vertex[0], vertex[1]);
					}
				}
			}
		}
		//Appends the new labels and ensures that there are no repeats
		for (String curr1 : valuesList) {
			if(!curr1.startsWith("adjList")) {
				String[] vertex2 = curr1.toString().split(",");
				String value = vertices.get(vertex2[0]);
				//Updates the label distribution for each vertex
				if ((value == null) || Double.parseDouble(value) < Double.parseDouble(vertex2[1])){
					labels.append(curr1.toString() + ":");
					vertices.put(vertex2[0] ,vertex2[1]);
				}
			}
		}
	context.write(new Text(key), new Text(labels.toString() + "!" + adjList.toString()));
   }
}
