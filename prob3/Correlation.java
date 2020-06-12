// CSC 369: Distributed Computing
// Quinn Coleman, Alex Pinto, Logan Anderson
// qcoleman@calpoly.edu, arpinto@calpoly.edu, lander24@calpoly.edu
// Map-Reduce COVID Correlation Calc


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
import java.lang.String;

// dCache imports
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.Set;
import java.util.Iterator;


public class Correlation {

// First MapReduce Mapper

public static class regionAggMapper     
     extends Mapper< Text, Text, Text, Text > {

HashMap<String, HashMap<Integer, Integer>> totals = new HashMap();
HashMap<Integer, Integer> slo_totals = new HashMap();
HashMap<Integer, Integer> sb_totals = new HashMap();
HashMap<Integer, Integer> mon_totals = new HashMap();
HashMap<Integer, Integer> sf_totals = new HashMap();
HashMap<Integer, Integer> sm_totals = new HashMap();
HashMap<Integer, Integer> sc_totals = new HashMap();
HashMap<Integer, Integer> alam_totals = new HashMap();
HashMap<Integer, Integer> cc_totals = new HashMap();
HashMap<Integer, Integer> la_totals = new HashMap();
HashMap<Integer, Integer> orange_totals = new HashMap();

//test
//int total = 0;

private int format_date(String str_date) {
   return Integer.parseInt(str_date.replace("-", ""));
}

protected void setup(Context context)
       throws IOException, InterruptedException{

  // retrieve distributed cache file info

try {

  URI cacheFiles[] = context.getCacheFiles();

  URI fileURI = cacheFiles[0];
  Path filePath = new Path(fileURI);

 //  USE getName() to extract file name!!!!!!
  File f = new File(filePath.getName());

  BufferedReader file = new BufferedReader(new FileReader(f));

  String line = "";
  //int smallest_date = null;
  //int yeah = 0;

  while ((line = file.readLine()) != null) {
     
    String[] record = line.split(",");
    // all_counts.put(String.valueOf(yeah), line);
    //yeah += 1;
    if (!record[0].contains("date")) {
      int key = format_date(record[0]);
      //if (smallest_date == null || smallest_date < key)
      //   smallest_date = key;
      String county = record[1];
      String state = record[2];
      int cases = Integer.parseInt(record[4]);
      //all_counts.put(key,value);
      if (state.contains("California")) {
	     if (county.contains("San Luis Obispo")) 
                 slo_totals.put(key, cases);	
	     else if (county.contains("Santa Barbara"))
                 sb_totals.put(key, cases);	
	     else if (county.contains("Monterey")) 
                 mon_totals.put(key, cases);	
	     else if (county.contains("San Francisco"))
                 sf_totals.put(key, cases);	 
	     else if (county.contains("San Mateo")) 
		 sm_totals.put(key, cases); 
	     else if (county.contains("Santa Clara")) 
		 sc_totals.put(key, cases); 
	     else if (county.contains("Alameda")) 
		 alam_totals.put(key, cases); 
	     else if (county.contains("Contra Costa")) 
		 cc_totals.put(key, cases); 
	     else if (county.contains("Los Angeles")) 
		 la_totals.put(key, cases); 
	     else if (county.contains("Orange"))
	         orange_totals.put(key, cases); 
      }
    }
  }
  // Initialize total hashmap
  totals.put("San Luis Obispo", slo_totals);	
  totals.put("Santa Barbara", sb_totals);	
  totals.put("Monterey", mon_totals);	
  totals.put("San Francisco", sf_totals);	
  totals.put("San Mateo", sm_totals);	
  totals.put("Santa Clara", sc_totals);	
  totals.put("Alameda", alam_totals);	
  totals.put("Contra Costa", cc_totals);	
  totals.put("Los Angeles", la_totals);	
  totals.put("Orange", orange_totals);	
 
}
 catch (Exception e) {
   context.write(new Text("ERROR"), new Text(e.toString()));

  }


} // setup


// Selection & projection
public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {

//    context.write(key, value);

  // total += 1;
	
   String date = key.toString();
   String[] record_list =  value.toString().split(",");  // extract the string object from value
   if (!date.contains("date")) {
      // Test
      //String val_len = String.valueOf(record_list.length);
      String county = record_list[0].trim();
      String state = record_list[1].trim();
     if (state.contains("California")) {
	      if (county.contains("San Luis Obispo") || county.contains("Santa Barbara") || 
		  county.contains("Monterey") || county.contains("San Francisco") || 
		  county.contains("San Mateo") || county.contains("Santa Clara") || county.contains("Alameda") ||
			 county.contains("Contra Costa") || county.contains("Los Angeles") || county.contains("Orange")) {
      		
		int cases = Integer.parseInt(record_list[3]);
		HashMap<Integer, Integer> county_map = totals.get(county);
      		int prev_date_int = format_date(date) - 1; 
     		int new_cases = county_map.containsKey(prev_date_int) ? (cases - county_map.get(prev_date_int)) : cases;       
      		context.write(key, new Text(String.join(",", county, String.valueOf(new_cases))));
	      } 
      }  
      
      // Length test
      //if (county.contains("Wood") && key.toString().contains("2020-05-19"))
      //   context.write(key, new Text(String.valueOf(all_counts.size())));
      
      // Name test
      //if (county.contains("Los Angeles")) 
      //   context.write(key, new Text(county));
   }

 } // map

// DEBUG
/*protected void cleanup(Context context)
      throws IOException, InterruptedException {

      for (Map.Entry<String,HashMap<Integer,Integer>> big_entry : totals.entrySet()) {
	  context.write(new Text(big_entry.getKey()), new Text("Here's the map contents"));
          for (Map.Entry<Integer,Integer> entry : big_entry.getValue().entrySet()) {
              context.write(new Text(String.valueOf(entry.getKey())), new Text(String.valueOf(entry.getValue())));
	  }
      }
} */

} //counterMapper

// First MapReduce Reducer

public static class regionAggCombiner  
      extends  Reducer< Text, Text, Text, Text> {

HashMap<String, Integer> cc_cases = new HashMap();
HashMap<String, Integer> bay_cases = new HashMap();
HashMap<String, Integer> la_cases = new HashMap();

@Override  

public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

  /*for (Text val : values) {
       context.write(key, val);
  } */ 
  String date = key.toString().trim();
  for(Text val : values) {
         String[] record = val.toString().split(",");
         String county = record[0];
	 Integer new_cases = Integer.parseInt(record[1]);
	 if (county.contains("San Luis Obispo") || county.contains("Santa Barbara") || 
		  county.contains("Monterey")) {
		 // Update contents
		 int curr_sum = cc_cases.containsKey(date) ? cc_cases.get(date) : 0;
		 curr_sum += new_cases; 
	         cc_cases.put(date, curr_sum);
         } else if (county.contains("San Francisco") || county.contains("San Mateo") ||
			 county.contains("Santa Clara") || county.contains("Alameda") ||
			 county.contains("Contra Costa")) {
		 int curr_sum = bay_cases.containsKey(date) ? bay_cases.get(date) : 0;
		 curr_sum += new_cases; 
	         bay_cases.put(date, curr_sum);
         } else if (county.contains("Los Angeles") || county.contains("Orange")) {
		 int curr_sum = la_cases.containsKey(date) ? la_cases.get(date) : 0;
		 curr_sum += new_cases; 
	         la_cases.put(date, curr_sum);
         }
  }

} // reduce

protected void cleanup(Context context)
      throws IOException, InterruptedException {
    
    for (Map.Entry<String,Integer> entry : cc_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text("Central Coast," + String.valueOf(entry.getValue())));
 
    for (Map.Entry<String,Integer> entry : bay_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text("Bay Area," + String.valueOf(entry.getValue())));
 
    for (Map.Entry<String,Integer> entry : la_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text("LA Area," + String.valueOf(entry.getValue()))); 

 } // cleanup 

} // reducer


public static class regionAggReducer  
      extends  Reducer< Text, Text, Text, Text> {

HashMap<Integer, Integer> cc_cases = new HashMap();
HashMap<Integer, Integer> bay_cases = new HashMap();
HashMap<Integer, Integer> la_cases = new HashMap();

private int format_date(String str_date) {
   return Integer.parseInt(str_date.replace("-", ""));
}
    
@Override  

public void reduce( Text key, Iterable<Text> values, Context context)
     throws IOException, InterruptedException {

 /*for (Text val : values) {
     context.write(key, val);
 }*/

 int date = format_date(key.toString());
 for(Text val : values) {
    String[] record = val.toString().split(",");
    String region = record[0];
    Integer new_cases = Integer.parseInt(record[1]);
    if (region.contains("Central Coast")) { 
        // Update contents
        int curr_sum = cc_cases.containsKey(date) ? cc_cases.get(date) : 0;
        curr_sum += new_cases; 
	cc_cases.put(date, curr_sum);
    } else if (region.contains("Bay Area")) { 
        int curr_sum = bay_cases.containsKey(date) ? bay_cases.get(date) : 0;
        curr_sum += new_cases; 
	bay_cases.put(date, curr_sum);
    } else if (region.contains("LA Area")) { 
        int curr_sum = la_cases.containsKey(date) ? la_cases.get(date) : 0;
        curr_sum += new_cases; 
	la_cases.put(date, curr_sum);	
    } 
 }
 } // reduce

protected void cleanup(Context context)
      throws IOException, InterruptedException {
      
    // prep data for pearson correlation computation
    int n_days = 67;	// 67 is the shortest-length amt of days (central coast)
    
    ArrayList<Integer> cc_dailies_old = new ArrayList();
    ArrayList<Map.Entry<Integer,Integer>> cc_dailies_p = new ArrayList(cc_cases.entrySet());
    cc_dailies_p.sort(Comparator.comparingInt(Map.Entry::getKey));
    for (Map.Entry<Integer,Integer> entry : cc_dailies_p)
        cc_dailies_old.add(entry.getValue());
    List<Integer> cc_dailies = cc_dailies_old.subList(cc_dailies_old.size() - n_days, cc_dailies_old.size());

    ArrayList<Integer> bay_dailies_old = new ArrayList();
    ArrayList<Map.Entry<Integer,Integer>> bay_dailies_p = new ArrayList(bay_cases.entrySet());
    bay_dailies_p.sort(Comparator.comparingInt(Map.Entry::getKey));
    for (Map.Entry<Integer,Integer> entry : bay_dailies_p)
        bay_dailies_old.add(entry.getValue());
    List<Integer> bay_dailies = bay_dailies_old.subList(bay_dailies_old.size() - n_days, bay_dailies_old.size());

    ArrayList<Integer> la_dailies_old = new ArrayList();
    ArrayList<Map.Entry<Integer,Integer>> la_dailies_p = new ArrayList(la_cases.entrySet());
    la_dailies_p.sort(Comparator.comparingInt(Map.Entry::getKey));
    for (Map.Entry<Integer,Integer> entry : la_dailies_p)
        la_dailies_old.add(entry.getValue());
    List<Integer> la_dailies = la_dailies_old.subList(la_dailies_old.size() - n_days, la_dailies_old.size());

       
    // perform pearson correlation computation
    int cc_sum = 0;
    for (Integer num : cc_dailies)
        cc_sum += num;
    double cc_mean = cc_sum / n_days;

    int bay_sum = 0;
    for (Integer num : bay_dailies)
        bay_sum += num;
    double bay_mean = bay_sum / n_days;

    int la_sum = 0;
    for (Integer num : la_dailies)
        la_sum += num;
    double la_mean = la_sum / n_days;

    // CC <-> BAY
    double numer = 0;
    double denom_sum1 = 0;
    double denom_sum2 = 0;
    for (int i = 0; i < n_days; i++) {
        numer += ((cc_dailies.get(i) - cc_mean) * (bay_dailies.get(i) - bay_mean));
        denom_sum1 += Math.pow((cc_dailies.get(i) - cc_mean), 2);
        denom_sum2 += Math.pow((bay_dailies.get(i) - bay_mean), 2);
    }
    double correlation = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
    context.write(new Text("pearson(Central Coast, Bay Area) ="), new Text(String.valueOf(correlation)));
  
    // CC <-> LA
    numer = 0;
    denom_sum1 = 0;
    denom_sum2 = 0;
    for (int i = 0; i < n_days; i++) {
        numer += ((cc_dailies.get(i) - cc_mean) * (la_dailies.get(i) - la_mean));
        denom_sum1 += Math.pow((cc_dailies.get(i) - cc_mean), 2);
        denom_sum2 += Math.pow((la_dailies.get(i) - la_mean), 2);
    }
    correlation = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
    context.write(new Text("pearson(Central Coast, Los Angeles) ="), new Text(String.valueOf(correlation)));
  
    // LA <-> BAY
    numer = 0;
    denom_sum1 = 0;
    denom_sum2 = 0;
    for (int i = 0; i < n_days; i++) {
        numer += ((la_dailies.get(i) - la_mean) * (bay_dailies.get(i) - bay_mean));
        denom_sum1 += Math.pow((la_dailies.get(i) - la_mean), 2);
        denom_sum2 += Math.pow((bay_dailies.get(i) - bay_mean), 2);
    }
    correlation = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
    context.write(new Text("pearson(Los Angeles, Bay Area) ="), new Text(String.valueOf(correlation)));
  
    /*context.write(new Text("Central Coast"), new Text(String.valueOf(cc_cases.size())));
    context.write(new Text("Bay"), new Text(String.valueOf(bay_cases.size())));
    context.write(new Text("LA"), new Text(String.valueOf(la_cases.size())));
    */
    /*for (Map.Entry<String,Integer> entry : cc_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text(",Central Coast," + String.valueOf(entry.getValue())));
 
    for (Map.Entry<String,Integer> entry : bay_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text(",Bay Area," + String.valueOf(entry.getValue())));
 
    for (Map.Entry<String,Integer> entry : la_cases.entrySet())
      context.write(new Text(entry.getKey()), new Text(",LA Area," + String.valueOf(entry.getValue()))); 
*/
 } // cleanup 

} // reducer


//  MapReduce Driver

public static void main(String[] args) throws Exception {

     Configuration conf = new Configuration();
     conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",","); // First comma in the line is the key - value separator
     
     Job  job = Job.getInstance(conf); 
     job.setJarByClass(Correlation.class);  
   
     URI dcacheFile = new URI("/data/us-covid-counties.csv");
     System.out.println(dcacheFile.toString());
     // set distributed cache file
     job.addCacheFile(dcacheFile);

     KeyValueTextInputFormat.addInputPath(job, new Path("/data/", "us-covid-counties.csv")); // put what you need as input file
     FileOutputFormat.setOutputPath(job, new Path("./test/","CovidCorrelation")); // put what you need as output file
     job.setInputFormatClass(KeyValueTextInputFormat.class);            // let's make input a CSV file

     job.setMapperClass(regionAggMapper.class);
     job.setCombinerClass(regionAggCombiner.class);
     job.setReducerClass(regionAggReducer.class);
     job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
     job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
     job.setJobName("CovidCorrelator 9000 II: Back with a Vengeance");
     System.exit(job.waitForCompletion(true) ? 0 : 1);

  } // main()


} // MyMapReduceDriver





