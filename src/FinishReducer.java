import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.PriorityQueue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class FinishReducer extends Reducer<Text, Text, Text, Text>{
	public void reduce(Text key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		if (values != null) {
			//Creates a new priority queue to enable the sorting of strings
			PriorityQueue<Pair> maximum = new PriorityQueue<Pair>(10, Collections.reverseOrder());
			for (Text v : values) {
				String[] vertices = v.toString().split(",");
				Pair currPair = new Pair(vertices[0], Double.parseDouble(vertices[1]));
				maximum.add(currPair);
			} 
		while(!maximum.isEmpty()) {
			Pair curr = maximum.remove();
			//Writes the key, probability pairs
			context.write(new Text("Friend of " + key.toString() + " " + curr.getString()), 
					 			    new Text("Probability " + Double.toString(curr.getProb())));
		}
	  }
   }
}
