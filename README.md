# Frequent-Itemsets-SON-Algorithm

## Objectives 
To identify frequent words in Yelp reviews. Implement the SON algorithm using the Apache Spark Framework in Scala om different datset sizes, test ,small and large. 


## Environment Requirements
- Scala 2.11 version
- Spark 2.3.1 version


## Dataset
Yelp Challenge Dataset.The dataset was prepared by extracting the text from reviews in Yelp Challenge Dataset Round 12.
All special characters were removed from the reviews by running ​sub(​​'\W+'​​, ​​''​​,word)​​ ​and then converting the words to lowercase. 
Duplicate entries and stop words were removed from a review and words were written to a file. Every word is written on an individual line with the format “review_index, word”. 
The small dataset (yelp_reviews_small) contains 10000 reviews and approximately 36000 unique words. The large dataset (yelp_reviews_large) contains 1 million reviews with approximately 620K words.

## Approach  
I have used 2 phase map reduce reduce for the implementation of the identifying frequent sets using the SON algorithm .
```
1. For the first phase , I have used A priori algorithm to get the local frequent itemsets. 

2. - I divided the input file into chunks and treated each chunk as a sample.
   - Then I ran the A priori algorithm on the sample using “ps” as threshold where s being the support of the entire file and p be the fraction the file is divided into. 
   - This yields local frequent itemsets from each chunk set and these form the candidate itemsets.

3. Then, I made a second pass through the baskets collecting the counts of these candidates and return the global frequent itemsets.
```

## Command to Run the code 
For Problem1 :
```
spark-submit --class Niharika_Gajam_SON Niharika_Gajam_HW3.jar <input_file.txt> <support_threshold> <output_file.txt>
```

For Problem2 :
I have used Item Based Collaborative Filtering Algorithm
```
spark-submit --class Niharika_Gajam_SON_small Niharika_Gajam_HW3.jar <input_file.txt> <support_threshold> <output_file.txt>
```
For Problem3 :
I have used Item Based Collaborative Filtering Algorithm
```
spark-submit --class Niharika_Gajam_SON_large Niharika_Gajam_HW3.jar <input_file.txt> <support_threshold> <output_file.txt>
```
