import java.io.*;
import java.util.*;

public class IowaLiquorSequential {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        String filename = "Iowa_Liquor_Sales.csv";

        File csvFile = new File(filename);
        if (csvFile.isFile()) {
            HashMap<String, ArrayList<Double>> yearToSales = new HashMap<String, ArrayList<Double>>();
            BufferedReader csvReader = new BufferedReader(new FileReader(filename));
            String row = "";
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                String[] date = data[1].split("/");

                if(data.length == 24 &&
                    data[20].length() > 0 &&
                    data[21].length() > 0 &&
                    data[22].length() > 0 &&
                    Character.isDigit(data[20].charAt(0)) && 
                    Character.isDigit(data[22].charAt(0)) &&
                    date.length == 3) {

                    int bottles = Integer.parseInt(data[20].replace("\"", ""));
                    double sales = Double.parseDouble(data[21].replace("\"", ""));
                    double vol = Double.parseDouble(data[22].replace("\"", "")) * bottles;

                    if(yearToSales.containsKey(date[2])) {
                        ArrayList<Double> salesData = yearToSales.get(date[2]);
                        salesData.set(0, salesData.get(0) + 1);
                        salesData.set(1, salesData.get(1) + vol);
                        salesData.set(2, salesData.get(2) + sales);
                    }
                    else {
                        ArrayList<Double> salesData = new ArrayList<Double>();
                        salesData.add(1.0);
                        salesData.add(vol);
                        salesData.add(sales);
                        yearToSales.put(date[2], salesData);
                    }
                }
            }
            csvReader.close();

            printRes(yearToSales);
        }

    }


    private static void printRes(HashMap<String, ArrayList<Double>> yearToSales) {
        for(HashMap.Entry<String, ArrayList<Double>> entry : yearToSales.entrySet()) {
            double numSales = entry.getValue().get(0);
            double totVol = entry.getValue().get(1);
            double totSales = entry.getValue().get(2);
            String resString = "Year: " + entry.getKey() + "      Number of sales: " + numSales + " Total Volume: " + totVol + " Total Sales: " + totSales;
            System.out.println(resString);
        }   
    }
}

