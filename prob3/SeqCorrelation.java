// CSC 369: Distributed Computing
// Quinn Coleman
// qcoleman@calpoly.edu
// Sequential COVID Correlation Calc

// Exception handling
import java.io.IOException;

// My imports
import java.util.*;
import java.lang.Math;
import java.lang.String;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;


public class SeqCorrelation {

// Utility
private static int format_date(String str_date) {
   return Integer.parseInt(str_date.replace("-", ""));
}

// First MapReduce Mapper
     
public static ArrayList<Map.Entry<Integer,String>> synthetic_map(String input_filepath) {

ArrayList<Map.Entry<Integer, String>> res = new ArrayList();
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


// Read in reference file
try {
File f_for_ref = new File(input_filepath);
BufferedReader file_for_ref = new BufferedReader(new FileReader(f_for_ref));
String line = "";
while ((line = file_for_ref.readLine()) != null) {
    String[] record = line.split(",");
    if (!record[0].contains("date")) {
      int key = format_date(record[0]);
      String county = record[1];
      String state = record[2];
      int cases = Integer.parseInt(record[4]);
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
} catch (Exception e) {
   System.out.println("READ ERROR: " + e.toString());
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

int counter = 0;
// MAP - read thru file
try {
File f = new File(input_filepath);
BufferedReader file = new BufferedReader(new FileReader(f));
String line = "";
while ((line = file.readLine()) != null) {
   String[] record = line.split(",");
   String date = record[0]; // key
   if (!date.contains("date")) {
      String county = record[1].trim();
      String state = record[2].trim();
     if (state.contains("California")) {
	      if (county.contains("San Luis Obispo") || county.contains("Santa Barbara") || 
		  county.contains("Monterey") || county.contains("San Francisco") || 
		  county.contains("San Mateo") || county.contains("Santa Clara") || county.contains("Alameda") ||
			 county.contains("Contra Costa") || county.contains("Los Angeles") || county.contains("Orange")) {
      		
		int cases = Integer.parseInt(record[4]);
		HashMap<Integer, Integer> county_map = totals.get(county);
      		int prev_date_int = format_date(date) - 1; 
     		int new_cases = county_map.containsKey(prev_date_int) ? (cases - county_map.get(prev_date_int)) : cases;       
	        res.add(new AbstractMap.SimpleEntry(format_date(date), String.join(",", county, String.valueOf(new_cases))));
		//System.out.println("map product: " + String.valueOf(format_date(date)) + " " + 
		//		String.join(",", county, String.valueOf(new_cases)));
	      	counter += 1;
	      } 
      }
   }
}
} catch (Exception e) {
   System.out.println("READ ERROR: " + e.toString());
}
//System.out.println("Size comp: " + String.valueOf(res.size()) + " - " + String.valueOf(counter));

return res;
}


public static HashMap<Integer,ArrayList<String>> shuffle(ArrayList<Map.Entry<Integer,String>> map_prod) {

HashMap<Integer, ArrayList<String>> res = new HashMap();

List<Map.Entry<Integer,String>> ordered_map_prod = new ArrayList(map_prod);
ordered_map_prod.sort(Comparator.comparingInt(Map.Entry::getKey));
ListIterator<Map.Entry<Integer,String>> it = ordered_map_prod.listIterator();

// System.out.println("Ordered Map Prod Size: " + String.valueOf(ordered_map_prod.size()));

int prev_date = -1;
ArrayList<String> common_vals = new ArrayList();
//for (Map.Entry<Integer,String> entry : ordered_map_prod) {
while (it.hasNext()) {
	Map.Entry<Integer,String> entry = it.next();
	int nextI = it.nextIndex();
	//System.out.println("Index: " + String.valueOf(nextI));
	//System.out.println("map product part (ordered): " + String.valueOf(entry.getKey()) + " " + String.valueOf(entry.getValue()));
	// Break-off check
	int date = entry.getKey();
	if (prev_date != -1 && date != prev_date) {
		res.put(prev_date, common_vals);
		//System.out.println("Shuffle config: " + String.valueOf(prev_date) + " " + String.valueOf(common_vals));
		common_vals = new ArrayList();
	}
	// Consider present pair	
        String curr_val = entry.getValue();
	common_vals.add(curr_val);	
	// Prep for break-off check	
	prev_date = date;

	// Last elem case
	if (nextI == ordered_map_prod.size()) {
		res.put(date, common_vals);
		// System.out.println("Last Date Found");
		// System.out.println(ordered_map_prod.size());	
		// System.out.println(ordered_map_prod.size() - 1);	
		// System.out.println("Ordered Map Prod Size - 1: " + String.valueOf(ordered_map_prod.size()-1));
		//System.out.println("Shuffle config: " + String.valueOf(date) + " " + String.valueOf(common_vals));
	}
}
// System.out.println("Res of shuffle keys: " + String.valueOf(res.keySet()));
//for (Map.Entry<Integer,ArrayList<String>> entry : res.entrySet()) {
//	System.out.println("After: " + String.valueOf(entry.getKey()) + " " + String.valueOf(entry.getValue()));
//}

return res;
}


public static void synthetic_reduce(HashMap<Integer,ArrayList<String>> ordered_prod, String out_filename) {

HashMap<Integer, Integer> cc_cases = new HashMap();
HashMap<Integer, Integer> bay_cases = new HashMap();
HashMap<Integer, Integer> la_cases = new HashMap();

for (Map.Entry<Integer,ArrayList<String>> entry : ordered_prod.entrySet()) {
	int key = entry.getKey();
	ArrayList<String> values = entry.getValue();
	// System.out.println("Key: " + String.valueOf(key));
	// System.out.println("Values: " + String.valueOf(values));
	// REDUCE
  	int date = key;
	for (String val : values) {
		String[] record = val.toString().split(",");
		String county = record[0];
		// System.out.println("County: " + county);
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
	//System.out.println("Out here");
}

// Cleanup - make correlation calc
// prep data for pearson correlation computation
int n_days = 67;	// 67 is the shortest-length amt of days (central coast)
/*
System.out.println("CC Size: " + String.valueOf(cc_cases.size()));
System.out.println("Bay Size: " + String.valueOf(bay_cases.size()));
System.out.println("LA Size: " + String.valueOf(la_cases.size()));
*/
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
double cc_bay_corr = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
//context.write(new Text("pearson(Central Coast, Bay Area) ="), new Text(String.valueOf(correlation)));
//System.out.println(cc_bay_corr);

// CC <-> LA
numer = 0;
denom_sum1 = 0;
denom_sum2 = 0;
for (int i = 0; i < n_days; i++) {
	numer += ((cc_dailies.get(i) - cc_mean) * (la_dailies.get(i) - la_mean));
	denom_sum1 += Math.pow((cc_dailies.get(i) - cc_mean), 2);
	denom_sum2 += Math.pow((la_dailies.get(i) - la_mean), 2);
}
double cc_la_corr = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
//context.write(new Text("pearson(Central Coast, Los Angeles) ="), new Text(String.valueOf(correlation)));
//System.out.println(cc_la_corr);

// LA <-> BAY
numer = 0;
denom_sum1 = 0;
denom_sum2 = 0;
for (int i = 0; i < n_days; i++) {
	numer += ((la_dailies.get(i) - la_mean) * (bay_dailies.get(i) - bay_mean));
	denom_sum1 += Math.pow((la_dailies.get(i) - la_mean), 2);
	denom_sum2 += Math.pow((bay_dailies.get(i) - bay_mean), 2);
}
double la_bay_corr = numer / (Math.sqrt(denom_sum1) * Math.sqrt(denom_sum2)); 
//context.write(new Text("pearson(Los Angeles, Bay Area) ="), new Text(String.valueOf(correlation)));
//System.out.println(la_bay_corr);

try {
//File fout = new File(out_filename);
BufferedWriter bw = new BufferedWriter(new FileWriter(out_filename));

bw.write("pearson(Central Coast, Bay Area) = " + String.valueOf(cc_bay_corr));
bw.newLine();
bw.write("pearson(Central Coast, Los Angeles) = " + String.valueOf(cc_la_corr));
bw.newLine();
bw.write("pearson(Los Angeles, Bay Area) = " + String.valueOf(la_bay_corr));
bw.newLine();
bw.close();

} catch (Exception e) {
   System.out.println("WRITE ERROR: " + e.toString());
}

}

public static void main(String[] args) throws Exception {
     ArrayList<Map.Entry<Integer,String>> map_prod = synthetic_map("/usr/data/us-covid-counties.csv");
     HashMap<Integer,ArrayList<String>> prod_shuffled = shuffle(map_prod);
     synthetic_reduce(prod_shuffled, "seq-correlation");

  } // main()


} // MyMapReduceDriver





