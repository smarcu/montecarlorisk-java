package com.intelliware.spark.montecarlorisk;

import org.apache.commons.math3.util.FastMath;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class KernelDensity {

	/**
	 * Given a set of samples form a distribution, estimates its density at the set of given points.
	 * Uses a Gaussian kernel with the given standard deviation.
	 */
	public static double[] estimate(JavaRDD<Double> samples, double standardDeviation, double[] evaluationPoints) {
		
		 double logStandardDeviationPlusHalfLog2Pi =
			      FastMath.log(standardDeviation) + 0.5 * FastMath.log(2 * FastMath.PI);

		 // (points, count)
		 Tuple2<double[], Integer> pointsTuple = samples.aggregate(new Tuple2<double[], Integer>((new double[evaluationPoints.length]), 0),
			 	
				 (x, y) -> {
			 		int i = 0;
			 		while (i < evaluationPoints.length) {
		 				x._1[i] += normPdf(y, standardDeviation, logStandardDeviationPlusHalfLog2Pi, evaluationPoints[i]);
		 		        i += 1;
			 		}
			 		return new Tuple2<double[], Integer>(x._1, i);
			 	},
			 	
			 	(x, y) -> {
		 			int i = 0;
		 			while (i < evaluationPoints.length) {
		 				x._1[i] += y._1[i];
		 				i += 1;
		 			}
			 		return new Tuple2<double[], Integer>(x._1, x._2 + y._2);
			 	}
		 	);
		 
		 	int i = 0;
 			while (i < pointsTuple._1.length) {
		      pointsTuple._1[i] /= pointsTuple._2;
		      i += 1;
		    }
		    return pointsTuple._1;
	}

	private static double normPdf(double mean, double standardDeviation, double logStandardDeviationPlusHalfLog2Pi, double x) {
	    double x0 = x - mean;
	    double x1 = x0 / standardDeviation;
	    return FastMath.exp(-0.5 * x1 * x1 - logStandardDeviationPlusHalfLog2Pi);
	}
	
}
