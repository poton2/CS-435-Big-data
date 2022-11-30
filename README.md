# CS-435-Big-data

# PA1
Generates the following unigram profiles (from a 1.5GB dataset consisting of 3000
books from the Gutenberg project s) using MapReduce

**(1) Profile 1-A**
A list of unigrams sorted in an alphabetical ascending order. Within the same unigram, the list should
be sorted by year in an ascending order.

**(2) Profile 1-B**
A list of unigrams sorted in alphabetical ascending order. Within the same unigram, the list should
be sorted by author’s last name in an ascending order.

**(3) Profile 2-A**
A list of bigrams sorted in an alphabetical ascending order. Within the same bigram, the list should
be sorted by year in an ascending order.

**(4) Profile 2-B**
A list of bigrams sorted in an alphabetical ascending order. Within the same bigram, the list should
be sorted by author’s last name in an ascending order. 

# PA2
The goal of this assignment is to take a given document, and extract sentences that best summarize the
document using TF-IDF. A document is any collection of words, and the set of all documents is called a corpus. 

# PA3 
Using Apache Spark 

**PageRank**

A sorted list of Wikipedia pages based on their ideal PageRank value in descending
order. Each row should contain the title of the article and its PageRank value. This computation is performed in my own Spark cluster with 5 machines with results from 25 iterations.

**Taxation**

A sorted list (in descending order) of Wikipedia pages based on their PageRank
value with taxation. Each row should contain the title of the article and its PageRank
value. This computation should be performed in my own Spark cluster with 5
machines with results from at least 25 iterations.
