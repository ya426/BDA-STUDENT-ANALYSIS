# Student Academic Performance Analysis Using Apache Spark RDDs

## Run
Install JDK 17 and sbt, then open a terminal in this folder.

```sh
sbt "run --ui"
```

For a run without the interactive UI hold: `sbt run`.
Open the Spark UI URL printed by the program (normally http://localhost:4040) while it is running. Under Jobs, select the lazy evaluation demonstration job and expand DAG Visualization. Inspect Stages for shuffle and Tasks for partition execution. Save screenshots before pressing Enter. If interactive input is unavailable in your IDE, run in a regular terminal.

## Assignment concepts
- Student case class represents ID, name, department, semester and marks.
- The small synthetic CSV is loaded into a Scala Vector and parallelized into four partitions.
- filter, map and reduceByKey are transformations: they define new RDDs. collect, mean and max are actions that trigger computation.
- Lazy evaluation defers transformations until an action. toDebugString exposes lineage without computing the pipeline.
- The department count pipeline has narrow filter/map dependencies followed by a wide reduceByKey dependency. A shuffle groups matching department keys and typically separates stages.
- Spark driver builds the DAG, schedules stages and sends partition tasks to executors. In local[4], four worker threads simulate parallel execution on one machine, not a physical cluster. In a real cluster, executors run on worker nodes and shuffle data across them.

## Expected results (calculated from CSV, not execution logs)
10 students; 6 above 75 (IDs 101,103,105,106,107,109).
Department counts: CE 2, CSE 3, ECE 3, ME 2.
Overall mean: 79.60. Highest: Ishita, 95.
High scorers by department: CE 1, CSE 3, ECE 1, ME 1.

## Dataset
`data/students.csv` is a synthetic dataset created for this assignment; no external original URL exists. Names and marks are illustrative.
