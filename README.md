# Hadoop Performance Benchmarking
A performance experiment on sequential v.s. MapReduce frameworks using Apache Hadoop. Experiments were run on the Cal Poly on-campus cluster. 

### View our [Lab Report](https://docs.google.com/document/d/1j5omkh1boeXXv9z7K9xetyM_Q-MoL19NoP6EpbJwG3s/edit?usp=sharing)
Quinn Coleman, Alex Pinto, Logan Anderson
qcoleman@calpoly.edu, arpinto@calpoly.edu, lander24@calpoly.edu
Lab 7 - CSC 369 Spring 2020

### Program Running Information

Problem 2:
- For IowaLiquorSequential, must have Iowa_Liquor_Sales.csv in the same directory

Problem 3:
- Output of Hadoop program "Correlation.java" goes to "./test/CovidCorrelation/"

Data Generation:
- Generate extra files of different size for "Correlation.java" by running "source gen_files.sh".
      IMPORTANT: In order to make larger files than original,
      copies of "us-covid-counties.csv" named "ucc-<record number>.csv" must be in your cwd.
      Files are lengthened/shortened by adding/removing lines to the original file.    

- Must manually type different input file into "Correlation.java" to use it.
      "CovidCorrelation.java" has no command line args.
