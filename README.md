# BigData_Session05Assignments
Mapreduce

We have a dataset of sales of different TV sets across different locations. Records look like:  
Samsung|Optima|14|Madhya Pradesh|132401|14200  
The fields are arranged like:  
Company Name|Product Name|Size in inches|State|Pin Code|Price  

1. Write a Map Reduce program to modify Task 2 (refer session 4, assignment 2) to use a custom partitioner with 4 reducers.
   Make sure that all records whose company name starts with A-F (upper or lower case) should go to 1st reducer,
   those starting with G-L to 2nd reducer, those starting with M-R to 3rd reducer and others to 4th reducer.
   Folder: Assignment51
   
2. Modify Task 2 (refer session 4, assignment 2) to take advantage of Combiner.
   Folder: Assignment52

3. Modify Task 3 (refer session 4, assignment 2) to take advantage of Combiner.  
   Folder: Assignment53
   
4. Write a Map Reduce program to view the total sales for each product for every Company corresponding to each size.
   Make sure that all records for a single company goes to a single reducer and inside every reducer,
   keys must be sorted in descending order of the size. 
   You may write a custom WritableComparable for this purpose.
   Folder: Assignment54
   
-----------------------------------------------------------------------------------------------------------------------------------
Titanic Dataset contains:
Column 1 : PassengerId 
Column 2 : Survived  (survived=0 & died=1) 
Column 3 : Pclass 
Column 4 : Name 
Column 5 : Sex 
Column 6 : Age 
Column 7 : SibSp 
Column 8 : Parch 
Column 9 : Ticket 
Column 10 : Fare 
Column 11 : Cabin 
Column 12 : Embarked 

Problem Statement 1: 
Find the average age of males and females who died in the Titanic tragedy.   
   Folder: Assignment55
   
Problem Statement 2: 
Find the number of people died or survived in each class with their genders and ages.
   Folder: Assignment56
