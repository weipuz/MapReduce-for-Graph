# MapReduce-for-Graph 
## Assignment 2 for CMPT732
#### Weipu Zhao 	301253897	 weipuz@sfu.ca
#### Shijie Li 	301157960	 shijiel@sfu.ca

Map reduce for graph for CMPT732 Assignment2

The objective of this exercise will be to use MapReduce to manipulate graphs. The dataset we will use is a graph of Wikipedia, in which nodes are pages, and their neighbors are the pages they link to. The dataset consists in two files:


- <code>/cs/bigdata/datasets/links-simple-sorted.txt)</code>
- <code>/cs/bigdata/datasets/titles-sorted.txt</code>

The first file (links-simple-sorted.txt) contains the graph per se. Here are the first lines of the file:
> 1: 1664968 <br>
2: 3 747213 1664968 1691047 4095634 5535664 <br>
3: 9 77935 79583 84707 564578 594898 681805 681886 835470 880698 1109091 1125108 1279972
1463445 1497566 1783284 1997564 2006526 2070954 2250217 2268713 2276203 2374802 2571397
2640902 2647217 2732378 2821237 3088028 3092827 3211549 3283735 3491412 3492254 3498305
3505664 3547201 3603437 3617913 3793767 3907547 4021634 4025897 4086017 4183126 4184025
4189168 4192731 4395141 4899940 4987592 4999120 5017477 5149173 5149311 5158741 5223097
5302153 5474252 5535280 <br>
4: 145 <br>
...

From the first line, we can deduce that page #1 contains a single link to page #1,664,968. Page #2 contains links to pages #3, #747,213, #1,664,968, #1,691,047, #4,095,634, #5,535,664, and so on. The second file (titles-sorted.txt) contains the list of titles of all Wikipedia pages, with the title of page #n being located at the nth line of the file.

### 1 Finding the shortest path
Please see the org.CMPT732.GraphMain.java and org.CMPT732.PageClass.java for source file;

Its a Hadoop program that finds the shortest path between two pages. It will take the identifiers of two pages as command-line arguments, and return the shortest path that goes the source page to the target page. For instance, going from page #2,111,690 (“Hadoop”) to page #4,371,964 (“SFU_Vancouver”) should give the following result:
> "Hadoop" (2111690) -> "2008" (88822) -> "Canadian_federal_election,_2008" (899021) -> "British_Columbia" (792600) -> "Simon_Fraser_University" (4605555) -> "SFU_Vancouver" (4371964)

The algorithm will run a series of MapReduce jobs over the graph: the output of each job will be fed as the input of the next one. You will have to make sure to set an upper bound on the number of iterations (<10, for instance) so that in case there is a bug in your code, you don’t end up with an infinite loop wasting the resources of the cluster.

The general idea of the algorithm is the following: the source page will initially have a distance of 0 while all other pages will have a distance of ∞. The first MapReduce iteration will make it so that all of the neighbors of the source page have a distance of 1. The second iteration will make it so that the neighbors’ neighbors will have a distance of 2 unless they have already been reached with a distance of 1. At each iteration, the algorithm memorizes the path that was used to reach all nodes. When the targed node is reached, the algorithm stops and returns the shortest path that was used to reach it. 


The command useage:  -inputfile –startpage –endpage:

For example, run:
<pre><code>hadoop jar MapreduceGraph.jar org.CMPT732.GraphMain /cs/bigdata/datasets/links-simple-sorted.txt 2111690 4371964</pre></code>

Will produce:

```
2111690 -> 5025110 -> 2616051 -> 1606305 -> 4605555 -> 4371964 
“Hadoop” -> “The_New_York_Times” -> “Joseph_Stalin” -> “Ensemble_Interpretation” -> “Simon_Fraser_University” -> “SFU_Vancouver”
```

### 2 Page Rank for Wiki pages
Please see the org.CMPT732.PageRankMain.java and org.CMPT732.PageRankClass.java for source file;

This program aim to calculate the PageRank over the Wikipedia graph.PageRank is the algorithm used by Google to rank its search results. The main idea of PageRank is that it gives a higher score to pages that have more inbound links. However, it penalizes inbound links that come from pages that have a high number of outbound links. Here is the formula PageRank uses for a node n:

![Alt text](http://upload.wikimedia.org/math/8/0/1/80125f33d12ceb608fdb9daec09d9c10.png)

where p_1, p_2, ..., p_N are the pages under consideration, M(p_i) is the set of pages that link to p_i, L(p_j) is the number of outbound links on page p_j, and N is the total number of pages.

In this formula, d is the damping factor, for which we will use the value 0.85.
Each vertex in the graph starts with a seed value, which is 1 divided by the number of nodes in the graph. At each iteration, each node propagates its value to all pages it links to, using the PageRank formula. Normally, the iterative process goes on until convergence is achieved, but in order to simplify things and to avoid wasting the cluster’s resources, in this exercise, we will only use five iterations.

The Mapper receives the identifier of a page as an argument, and information about the page as its value, as was the case in the shortest path algorithm.

The Reducer goes through adds up the values received from all neighbors, and uses the PageRank formula to update the PageRank of the current page. 

Like the question 1, we implemented a PageRankClass to represent a page with two attributes: rank and neighbors;
```java
private double rank;
private ArrayList<Integer> neighbors = new ArrayList<Integer>();
```
Head of the result of fifth iterations:
```
5302153 	United_States			0.0022770546315746347,
84707 		2007				0.0014527500559384239,
88822 		2008				0.0014038698921997781,
1921890  	Geographic_coorinate_system	0.0013067056979026398,
5300058   	United_Kindom			0.0010291567437501275,
81615 	  	2006				8.902316372056214E-4,
1804986 	 France				7.357219268942865E-4,
5535280  	Wikimedia_Commons		7.338271902724862E-4,
896161 	 	Canada				6.698527391395864E-4,
5535664 	 Wiktionary 			6.418377439061107E-4,


