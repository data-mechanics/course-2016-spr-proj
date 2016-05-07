Yihong Guo (Billguo@bu.edu)

To run:

1. run project1.py  

2. run "mongoexport -d repo -c billguo.test -o test.json" to output json file 

3. I have already doen these two and ouput a json file that will work with index1.html and index2.html  

Datasets used:
Employee Earnings Report 2012
Employee Earnings Report 2013
Employee Earnings Report 2014

Description:
In this project I'm focused on individual earning reports in three continous years.
By compare the total earnings each year and the number of person who has increasing total earnings in three years, we can find 
out the percentage of people who earns more and more each year and the tile and departments of these peoples.

To finish this, I go through three collections and looking for data that has same name. Than I merge all three collections and 
put total earnings of three years in a same collection.

For the second part, I go through all three collections and looks for people who have increasing total earning each year and  
only keep these data.

Then we calculate the percentage of people who have increasing earnings each year with the number of items of frist script and
the number of items of second script.

Visualization:
I have done two visualization index1 and index2 and have written a script that will generate a json file in a correct format. I can simply change the original database(and results from project1) input in the scipt to change the input of visualization. Since the results from my project1 are small, I used earnings report 2014 as a sample input and visualize it.

Original structure:
"total_earnings" : "100381.19",
  "zip" : "02132",
  "detail" : "0.00",
  "injured" : "0.00",
  "title" : "Supvising Claims Agent (Asd)",
  "other" : "1842.87",
  "regular" : "98538.32",
  "name" : "Adario,Anthony J",
  "retro" : "0.00",
  "department_name" : "ASD Human Resources",
  "overtime" : "0.00",
  "quinn" : "0.00"


Output structure: (Sample)
"name" : "job&earn2014", 
"children" : [ { 
	"name" : "ASD Human Resources",
	"children" : [ { 
		"name" : "Supvising Claims Agent (Asd)", 
		"children" : [ { 
			"name" : "Adario,Anthony J", "total_earnings" : "100381.19" } ] }
			
More information about the result can be found in project report.pdf

I have updated most of the project and include detailed description in the project report.pdf Due to a problem I don't know how to insert picture in the readme file, I list them in the report as well.
