# montecarlorisk-java
Spark Risk Monte Carlo - Java

Java version of https://github.com/sryza/montecarlorisk

To buid a jar:

	mvn clean package
	
To run single process locally:

	spark-submit --class com.intelliware.spark.montecarlorisk.MonteCarloRisk --master local \
		target/montecarlorisk-java-0.0.1-SNAPSHOT.jar \
		<instruments file> <num trials> <parallelism> <factor means file> <factor covariances file>

Replace "--master local" with "--master spark://<master host>:<master port>"

If the cluster is running YARN, you can replace "--master local" with "--master yarn".