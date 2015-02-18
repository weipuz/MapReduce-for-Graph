# MapReduce-for-Graph
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
