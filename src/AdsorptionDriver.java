import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Driver class of program
public class AdsorptionDriver {
  public static int main(String[] args) throws Exception { 
	  if(args.length < 5) {
		  System.err.println("Incorrect argument");
		  return -1;
	  }
	  //Parses the command line data
	  String task = args[0];
	  String input = args[1];
	  String out1 = args[2];
	  String out2 = args[3];
	  String numReducers = args[4];
	  //Gets the job
	  Job job = Job.getInstance();
	  job.setJarByClass(AdsorptionDriver.class);
	  //Sets mapper and reducer job types
	  job.setMapOutputKeyClass(Text.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(Text.class);
	  //Goes through each of the four cases
	  if (task.equals("init")) {
		  job.setMapperClass(InitMapper.class);
		  job.setReducerClass(InitReducer.class);
		  FileInputFormat.addInputPath(job, new Path(input));
		  AdsorptionDriver.deleteDirectory(out1);
		  FileOutputFormat.setOutputPath(job, new Path(out1));
		  job.setNumReduceTasks(Integer.parseInt(numReducers));
		  return job.waitForCompletion(true) ? 0:1;
	  }
	  else if (task.equals("iter")) {
		  job.setMapperClass(IterMapper.class);
		  job.setReducerClass(IterReducer.class);
		  FileInputFormat.addInputPath(job, new Path(out1));
		  AdsorptionDriver.deleteDirectory(out2);
		  FileOutputFormat.setOutputPath(job, new Path(out2));
		  job.setNumReduceTasks(Integer.parseInt(numReducers));
		  return job.waitForCompletion(true)? 0:1;
	  }
	  else if (task.equals("finish")) {
		  job.setMapperClass(FinishMapper.class);
		  job.setReducerClass(FinishReducer.class);
		  FileInputFormat.addInputPath(job, new Path(out2));
		  AdsorptionDriver.deleteDirectory(out1);
		  FileOutputFormat.setOutputPath(job, new Path(out1));
		  job.setNumReduceTasks(1);
		  return job.waitForCompletion(true) ? 0:1;
	  }
    else if (task.equals("composite")) {
    	AdsorptionDriver.main(new String[] {"init", input, out1, out2, numReducers});
    	int counter = 0;
    	//Iterates between diff and iter while diff is greater than 30
    	while (counter < 12) {
    		AdsorptionDriver.main(new String[] {"iter", input, out1, out2, numReducers});
    		AdsorptionDriver.main(new String[] {"iter", input, out2, out1, numReducers});
    		counter++;
    	}	
    	AdsorptionDriver.main(new String[] {"finish", input, out2, out1, numReducers});
    	return 0;
    }
    System.err.println("Incorrect argument");
    return -1;
  }

  static void deleteDirectory(String path) throws Exception {
	    Path todelete = new Path(path);
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(path),conf);
	    
	    if (fs.exists(todelete)) 
	      fs.delete(todelete, true);
	      
	    fs.close();
	  }
}
  

