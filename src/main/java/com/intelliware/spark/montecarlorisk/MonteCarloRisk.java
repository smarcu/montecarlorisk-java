package com.intelliware.spark.montecarlorisk;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class MonteCarloRisk {
	
	public static void main(String s[]) throws Exception {
		SparkConf conf = new SparkConf().setAppName("MonteCarloRiskJava").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Instrument[] instruments = readInstruments(s[0]);
		Arrays.stream(instruments).forEach(i -> System.out.println(i));
		int numTrials = Integer.parseInt(s[1]);
		int parallelism = Integer.parseInt(s[2]);
		Double[] factorMeans = readMeans(s[3]);
		Arrays.stream(factorMeans).forEach(i -> System.out.println(i));
		
		Double[][] factorCovariances = readCovariances(s[4]);
		Arrays.stream(factorCovariances).forEach(i -> System.out.println(Arrays.toString(i)));
	    long seed = (s.length > 5) ? Long.parseLong(s[5]) : System.currentTimeMillis();
		
		Broadcast<Instrument[]> broadcastInstruments = sc.broadcast(instruments);
	}
	
	
	
	public static Instrument[] readInstruments(String instrumentsFile) throws IOException {
		Stream<Instrument> instruments = Files.lines(Paths.get(instrumentsFile))
				.map(line -> line.split(","))
				.map(lineStrArray -> Arrays.stream(lineStrArray).map(Double::parseDouble).toArray(Double[]::new))
				.map(lineDoubleArray -> new Instrument(
											Arrays.copyOfRange(lineDoubleArray, 2, lineDoubleArray.length), 
											lineDoubleArray[0], 
											lineDoubleArray[1]));
		return instruments.toArray(Instrument[]::new);
	}
	
	public static Double[] readMeans(String meansFile) throws IOException {
		Stream<Double> means = Files.lines(Paths.get(meansFile)).map(Double::parseDouble);
		return means.toArray(Double[]::new);
	}
	
	public static Double[][] readCovariances(String covsFile) throws IOException {
		Stream<Double[]> instruments = Files.lines(Paths.get(covsFile))
				.map(line -> line.split(","))
				.map(lineStrArray -> Arrays.stream(lineStrArray).map(Double::parseDouble).toArray(Double[]::new));
		return instruments.toArray(Double[][]::new);
	}
	
	
}
