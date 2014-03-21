# dga-graphx 

- GraphX Algorithms

The dga-graphX package contains several pre-built executable graph algorithms built on Spark using the GraphX framework.  

### pre-requisites

 * [Spark]  (http://spark.apache.org/)   0.9.0 or later
 * [graphX]  (http://spark.apache.org/docs/latest/graphx-programming-guide.html)   
 * [Gradle] (http://www.gradle.org/) 

### build

If necessary edit the build.gradle file to set your version of spark and graphX

> gradle clean dist

Check the build/dist folder for dga-graphx-0.1.jar.   


# Algorithms 

## Louvain

### about louvain

Louvain distributed community detection is a parallelized version of this work:
```
Fast unfolding of communities in large networks, 
Vincent D Blondel, Jean-Loup Guillaume, Renaud Lambiotte, Etienne Lefebvre, 
Journal of Statistical Mechanics: Theory and Experiment 2008 (10), P10008 (12pp)
```
In the original algorithm each vertex examines the communities of its neighbors and makes a chooses a new community based on a function to maximize the calculated change in modularity.  In the distributed version all vertices make this choice simultaneously rather than in serial order, updating the graph state after each change.  Because choices are made in parallel some choice will be incorrect and will not maximize modularity values, however after repeated iterations community choices become more stable and we get results that closely mirror the serial algorithm.

### running louvain

After building the package (See above) you can execute the lovain algorithm against an edge list using the provided script

```
bin/louvain

Usage: class com.soteradefense.dga.graphx.louvain.Main$ [options] [<property>=<value>....]

  -i <value> | --input <value>
        input file or path  Required.
  -o <value> | --output <value>
        output path Required
  -m <value> | --master <value>
        spark master, local[N] or spark://host:port default=local
  -h <value> | --sparkhome <value>
        SPARK_HOME Required to run on cluster
  -n <value> | --jobname <value>
        job name
  -p <value> | --parallelism <value>
        sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions
  -x <value> | --minprogress <value>
        Number of vertices that must change communites for the algorithm to consider progress. default=2000
  -y <value> | --progresscounter <value>
        Number of times the algorithm can fail to make progress before exiting. default=1
  -d <value> | --edgedelimiter <value>
        specify input file edge delimiter. default=","
  -j <value> | --jars <value>
        comma seperated list of jars
  -z <value> | --ipaddress <value>
        Set to true to convert ipaddresses to Long ids. Defaults to false
  <property>=<value>....
```

To run a small local example execute:
```
bin/louvain -i examples/small_edges.tsv -o test_output --edgedelimiter "\t" 2> stderr.txt
```

Spark produces alot of output, so sending stderr to a log file is recommended.  Examine the test_output folder. you should see

```
test_output/
├── level_0_edges
│   ├── _SUCCESS
│   └── part-00000
├── level_0_vertices
│   ├── _SUCCESS
│   └── part-00000
└── qvalues
    ├── _SUCCESS
    └── part-00000
```

```
cat test_output/level_0_vertices/part-00000 
(7,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(4,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(2,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(6,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:4})
(8,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(5,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(9,{community:8,communitySigmaTot:13,internalWeight:0,nodeWeight:3})
(3,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:4})
(1,{community:4,communitySigmaTot:21,internalWeight:0,nodeWeight:5})

cat test_output/qvalues/part-00000 
(0,0.4134948096885813)
```

Note: the output is laid out as if you were in hdfs even when running local.  For each level you see an edges directory and a vertices directory.   The "level" refers to the number of times the graph has been "community compressed".  At level 1 all of the level 0 vertices in community X are represented by a single vertex with the VertexID: X.  For the small example all modulairyt was maximized with no community compression so only level 0 was computed.  The vertices show the state of each vertex while the edges file specify the graph structure.   The qvalues directory lists the modularity of the graph at each level of compression.  For this example you should be able to see all of vertices splitting off into two distinct communities (community 4 and 8 ) with a final qvalue of ~ 0.413


### running louvain on a cluster

To run on a cluster be sure your input and output paths are of the form "hdfs://<namenode>/path" and ensure you provide the --master and --sparkhome options.  The --jars option is already set by the louvain script itself and need not be applied.

### parallelism

To change the level of parallelism use the -p or --parallelism option.  If this option is not set parallelism will be based on the layout of the input data in HDFS.  The number of partitions of the input file sets the level of parallelism.   

### advanced

If you would like to include the louvain algorithm in your own compute pipeline or create a custom output format, etc you can easily do so by extending the com.soteradefense.dga.graphx.louvain.LouvainHarness class.  See HDFSLouvainRunner which extends LouvainHarness and is called by Main for the example above

