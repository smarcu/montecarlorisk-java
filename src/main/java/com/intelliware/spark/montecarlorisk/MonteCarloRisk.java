package com.intelliware.spark.montecarlorisk;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.random.MersenneTwister;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
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
		double[] factorMeans = readMeans(s[3]);
		Arrays.stream(factorMeans).forEach(i -> System.out.println(i));
		
		double[][] factorCovariances = readCovariances(s[4]);
		Arrays.stream(factorCovariances).forEach(i -> System.out.println(Arrays.toString(i)));
	    long seed = (s.length > 5) ? Long.parseLong(s[5]) : System.currentTimeMillis();
		
		Broadcast<Instrument[]> broadcastInstruments = sc.broadcast(instruments);
		
		// Generate different seeds so that our simulations don't all end up with the same results
		List<Long> seeds = LongStream.range(seed, seed + parallelism).boxed().collect(Collectors.toList());
		
		JavaRDD<Long> seedRdd = sc.parallelize(seeds, parallelism);
		
		// Main computation: run simulations and compute aggregate return for each
		JavaRDD<Double> trialsRdd = seedRdd.flatMap(sdd -> 
			Arrays.asList(ArrayUtils.toObject(trialValues(sdd, numTrials/parallelism, broadcastInstruments.value(), 
														factorMeans, factorCovariances))));

		// Cache the results so that we don't recompute for both of the summarizations below
	    trialsRdd.cache();
	    
	    // Calculate VaR
	    List<Double> varFivePercentList = trialsRdd.takeOrdered(Math.max(numTrials / 20, 1));
	    System.out.println("VaR List: "+ varFivePercentList);
	    Double varFivePercent = (Double)varFivePercentList.get(varFivePercentList.size()-1);
	    System.out.println("VaR: "+ varFivePercent);
	    
	    // Kernel density estimation
	    
	    List<Double> domainList = new ArrayList<>();
	    for(double d = 20; d <= 60; d += .2) domainList.add(d);
	    double [] domain = ArrayUtils.toPrimitive(domainList.toArray(new Double[0]));

	    double[] densities = KernelDensity.estimate(trialsRdd, 0.25, domain);

	    try (PrintWriter pw = new PrintWriter("densities.csv")) {
	    	for (int i=0; i<domain.length; i++) {
	    		pw.println(domain[i] + ", " + densities[i]);
	    	}
	    }
	    
	}
	

	public static double[] trialValues(long seed, int numTrials, Instrument[] instruments, 
			double[] factorMeans, double[][] factorCovariances) {

		Arrays.stream(factorCovariances).forEach(i -> System.out.println(Arrays.toString(i)));
		
		MersenneTwister rand = new MersenneTwister();
		MultivariateNormalDistribution multivariateNormal = new MultivariateNormalDistribution(rand, 
				factorMeans, factorCovariances);
		
		double trialValues[] = new double[numTrials];
		
		IntStream.range(0,  numTrials).forEach(i -> {
			double[] trial = multivariateNormal.sample();
			trialValues[i] = trialValue(trial, instruments);
		});
		
		System.out.println("trialValues: "+Arrays.toString(trialValues));
		
		return trialValues;
	}
	
	/**
	 * Calculate the full value of the portfolio under particular trial conditions.
	 */
	public static double trialValue(double[] trial, Instrument[] instruments) {
		double totalValue = 0;
		for(Instrument instrument : instruments) {
			totalValue += instrumentTrialValue(instrument, trial);
		}
		return totalValue;
	}
	
	 /**
	  * Calculate the value of a particular instrument under particular trial conditions.
	  */
	public static double instrumentTrialValue(Instrument instrument, double[] trial) {
		double instrumentTrialValue = 0;
		int i=0;
		while (i < trial.length) {
			instrumentTrialValue += trial[i] * instrument.getFactorWeights()[i];
			i += 1;
		}
		return Math.min(Math.max(instrumentTrialValue, instrument.getMinValue()), instrument.getMaxValue());
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
	
	public static double[] readMeans(String meansFile) throws IOException {
		Stream<Double> means = Files.lines(Paths.get(meansFile)).map(Double::parseDouble);
		return ArrayUtils.toPrimitive(means.toArray(Double[]::new));
	}
	
	public static double[][] readCovariances(String covsFile) throws IOException {
		Stream<double[]> instruments = Files.lines(Paths.get(covsFile))
				.map(line -> line.split(","))
				.map(lineStrArray -> ArrayUtils.toPrimitive(Arrays.stream(lineStrArray).map(Double::parseDouble).toArray(Double[]::new)));
		return instruments.toArray(double[][]::new);
	}
	
}
