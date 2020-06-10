


// CSC 369: Distributed Computing
// Alex Dekhtyar

// Java Hadoop Template

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


public class IowaLiquor {


// Mapper  Class Template


public static class SwitchMapper 
     extends Mapper< Text, Text, Text, Text > {

@Override   
public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] valueStr = value.toString().split(",");
    String[] date = valueStr[0].split("/");

    if(valueStr.length == 24 &&
        valueStr[19].length() > 0 &&
        valueStr[20].length() > 0 &&
        valueStr[21].length() > 0 &&
        Character.isDigit(valueStr[19].charAt(0)) && 
        Character.isDigit(valueStr[21].charAt(0)) &&
        valueStr[20].charAt(0) == '$' &&
        date.length == 3) {

        String year = date[2];
        double volume = Double.parseDouble(valueStr[21]) * Double.parseDouble(valueStr[19]);
        String sales = valueStr[20].substring(1, valueStr[20].length());

        String resString = "" + volume + "," + sales;

        context.write(new Text(year), new Text(resString));
    }


 } // map


} // MyMapperClass


//  Reducer Class Template

public static class SwitchReducer   // needs to replace the four type labels with actual Java class names
      extends  Reducer< Text, Text, Text, Text> {

@Override  

public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

    double totSales = 0.0;
    double totVol = 0.0;
    int numSales = 0;

    for(Text value : values) {
        String[] valueArr = value.toString().split(",");
        totVol += Double.parseDouble(valueArr[0]);
        totSales += Double.parseDouble(valueArr[1]);
        numSales++;
    }

    String resString = "Number of sales: " + numSales + " Total Volume: " + totVol + " Total Sales: " + totSales;

    context.write(key, new Text(resString));

 } // reduce


} // reducer


//  MapReduce Driver


  // we do everything here in main()
  public static void main(String[] args) throws Exception {

     // Step 0: let's  get configuration

      Configuration conf = new Configuration();
      conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",","); // learn to process two-column CSV files.


     // step 1: get a new MapReduce Job object
     Job  job = Job.getInstance(conf);  //  job = new Job() is now deprecated
     
    // step 2: register the MapReduce class
      job.setJarByClass(IowaLiquor.class);  

   //  step 3:  Set Input and Output files
       KeyValueTextInputFormat.addInputPath(job, new Path("/user/arpinto", "liquorSmall.csv")); // put what you need as input file
       FileOutputFormat.setOutputPath(job, new Path("./test/","iowa-liquorSales")); // put what you need as output file
       job.setInputFormatClass(KeyValueTextInputFormat.class);            // let's make input a CSV file

   // step 4:  Register mapper and reducer
      job.setMapperClass(SwitchMapper.class);
      job.setReducerClass(SwitchReducer.class);
  
   //  step 5: Set up output information
       job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
       job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value

   // step 6: Set up other job parameters at will
      job.setJobName("iowa sales");


   // step 8: profit
      System.exit(job.waitForCompletion(true) ? 0:1);


  } // main()


} // MyMapReduceDriver



