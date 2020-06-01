
// CSC 369: Distributed Computing
// Quinn Coleman
// qcoleman@calpoly.edu

//  Two Hadoop Jobs Chained together

// Section 1: Imports
import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // input format for key-value files
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object

// Exception handling

import java.io.IOException;

// My imports
import java.util.*;
import java.lang.Math;

public class Correlation {

// First MapReduce Mapper

public static class counterMapper     
     extends Mapper< Text, Text, Text, Text > {
	
// Selection & projection
public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
	
   String[] record_list =  value.toString().split(",");  // extract the string object from value
   // Data mending - comma in vendor name and/or item desc safe-check
   if (record_list.length > 7) {
      String[] record_list_2 = new String[7];
      // Case - both
      if (record_list.length == 9) {
         String[] name_list = {record_list[2] + record_list[3]};
         String[] desc_list = {record_list[5] + record_list[6]};
         System.arraycopy(Arrays.copyOfRange(record_list, 0, 2), 0, record_list_2, 0, 2);
         System.arraycopy(name_list, 0, record_list_2, 2, 1);
         System.arraycopy(Arrays.copyOfRange(record_list, 4, 5), 0, record_list_2, 3, 1);
         System.arraycopy(desc_list, 0, record_list_2, 4, 1);
         System.arraycopy(Arrays.copyOfRange(record_list, 7, 9), 0, record_list_2, 5, 2);
      } else {
         // Case - item desc
         try {
            int test_num = Integer.parseInt(record_list[3]);
            String[] desc_list = {record_list[4] + record_list[5]};
            System.arraycopy(Arrays.copyOfRange(record_list, 0, 4), 0, record_list_2, 0, 4);
            System.arraycopy(desc_list, 0, record_list_2, 4, 1);
            System.arraycopy(Arrays.copyOfRange(record_list, 6, 8), 0, record_list_2, 5, 2);
         // Case - vendor name
         } catch(Exception e) {
            String[] name_list = {record_list[2] + record_list[3]};
            System.arraycopy(Arrays.copyOfRange(record_list, 0, 2), 0, record_list_2, 0, 2);
            System.arraycopy(name_list, 0, record_list_2, 2, 1);
            System.arraycopy(Arrays.copyOfRange(record_list, 4, 8), 0, record_list_2, 3, 4);
         }
      }
      record_list = record_list_2;
   }
   String bottle_num = record_list[5] == "null" ? "0" : record_list[5];
   String county = record_list[1];
   String desc = record_list[4];

   // Empty strings are two chars, b/c strings keep '' in string
   if (county.length() > 2) {
      if (desc.contains("vodka") || desc.contains("Vodka")) {
         Text new_key = new Text(county.substring(1, county.length() - 1));
         Text new_val = new Text(String.join(",", "V\t", bottle_num));
         context.write(new_key, new_val);
      } else if (desc.contains("rum") || desc.contains("Rum")) {
         Text new_key = new Text(county.substring(1, county.length() - 1));
         Text new_val = new Text(String.join(",", "R\t", bottle_num));
         context.write(new_key, new_val);
      }
   }

 } // map

} //counterMapper

// First MapReduce Reducer

public static class counterReducer  
      extends  Reducer< Text, Text, Text, Text> {

@Override  

public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

// output the word with the number of its occurrences
  long vodka_sum = 0; 
  long rum_sum = 0;

 for(Text val : values) {
    String[] items = val.toString().split(",");
    if (items[0].contains("V")) 
        vodka_sum += Long.parseLong(items[1]);
    else
        rum_sum += Long.parseLong(items[1]);
 } 
 context.write(key, new Text("," + String.join(",", String.valueOf(vodka_sum), String.valueOf(rum_sum))));

 } // reduce
} // reducer


// MapReduce Mapper 2

public static class correlationMapper  
     extends Mapper< Text, Text, Text, Text > {

    private List vodka_county_list = new ArrayList<String>();
    private List rum_county_list = new ArrayList<String>();
     
    public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

      //context.write(key, value);
      
      String[] record_list =  value.toString().split(",");  // extract the string object from value
      vodka_county_list.add(record_list[0]);
      rum_county_list.add(record_list[1]);

 } // map

    protected void cleanup(Context context)
      throws IOException, InterruptedException {

      Text key1 = new Text("Vodka");
      Text val1 = new Text(String.join(",", vodka_county_list));
      context.write(key1, val1);

      Text key2 = new Text("Rum");
      Text val2 = new Text(String.join(",", rum_county_list));
      context.write(key2, val2);

 }

} // correlationMapper

// MapReduce Reducer 2

public static class correlationReducer 
      extends  Reducer< Text, Text, Text, Text> {

    private int n_counties = 98;
    private List<Integer> rum_nums = new ArrayList<Integer>();
    private List<Integer> vodka_nums = new ArrayList<Integer>();

@Override

public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

    // perform pearson correlation computation
    if (key.toString().contains("Rum")) {
        for(Text val : values) {
	    for (String num: val.toString().split(","))
	    	rum_nums.add(Integer.parseInt(num));	
	}
    } else {
        for(Text val : values) {
	    for (String num: val.toString().split(","))
	    	vodka_nums.add(Integer.parseInt(num));
	}
    } 
    

 } // reduce

protected void cleanup(Context context)
      throws IOException, InterruptedException {
    
    int rum_sum = 0;
    for (Integer num : rum_nums)
        rum_sum += num;
    double rum_mean = rum_sum / n_counties;

    int vodka_sum = 0;
    for (Integer num : vodka_nums)
        vodka_sum += num; 
    double vodka_mean = vodka_sum / n_counties;
    
    double numer = 0;
    double denom_sum1 = 0;
    double denom_sum2 = 0;
    for (int i = 0; i < n_counties; i++) {
        numer += ((rum_nums.get(i) - rum_mean) * (vodka_nums.get(i) - vodka_mean));
        denom_sum1 += Math.pow((rum_nums.get(i) - rum_mean), 2);
        denom_sum2 += Math.pow((vodka_nums.get(i) - vodka_mean), 2);
    }

    double correlation = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
      
    context.write(new Text("pearson(rum, vodka) ="), new Text(String.valueOf(correlation)));
 }

} // reducer



//  MapReduce Driver

public static void main(String[] args) throws Exception {

     Configuration conf = new Configuration();
     conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",","); // First comma in the line is the key - value separator
     
     // MapReduce Job #1

     Job  job = Job.getInstance(conf); 
     job.setJarByClass(Correlation.class);  

     KeyValueTextInputFormat.addInputPath(job, new Path("/data/", "iowa.csv")); // put what you need as input file
     FileOutputFormat.setOutputPath(job, new Path("./test/","county_counts")); // put what you need as output file
     job.setInputFormatClass(KeyValueTextInputFormat.class);            // let's make input a CSV file

     job.setMapperClass(counterMapper.class);
     job.setReducerClass(counterReducer.class);
     job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
     job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
     job.setJobName("Count");
     job.waitForCompletion(true);

 // MapReduce Job #2

   Job corrJob = Job.getInstance(conf);
   corrJob.setJarByClass(Correlation.class);

   // Note how we are chaining inputs
   KeyValueTextInputFormat.addInputPath(corrJob, new Path("./test/county_counts", "part-r-00000")); // put what you need as input file
   FileOutputFormat.setOutputPath(corrJob, new Path("./test/","Correlation")); 
   corrJob.setInputFormatClass(KeyValueTextInputFormat.class);            // let's make input a CSV file

   corrJob.setMapperClass(correlationMapper.class);
   corrJob.setReducerClass(correlationReducer.class);
   corrJob.setOutputKeyClass(Text.class);
   corrJob.setOutputValueClass(Text.class);
   corrJob.setJobName("Correlation Baby!");
    
   System.exit(corrJob.waitForCompletion(true) ? 0: 1);
   

  } // main()


} // MyMapReduceDriver





