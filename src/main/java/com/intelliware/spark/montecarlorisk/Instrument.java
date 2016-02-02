package com.intelliware.spark.montecarlorisk;

import java.io.Serializable;
import java.util.Arrays;

public class Instrument implements Serializable {

	private static final long serialVersionUID = 1L;
	private Double[] factorWeights;
	private Double minValue = 0d;
	private Double maxValue = Double.MAX_VALUE;
	
	public Instrument(Double[] factorWeights, Double minValue, Double maxValue) {
		super();
		this.factorWeights = factorWeights;
		this.minValue = minValue;
		this.maxValue = maxValue;
	}

	public Double[] getFactorWeights() {
		return factorWeights;
	}
	public void setFactorWeights(Double[] factorWeights) {
		this.factorWeights = factorWeights;
	}
	public Double getMinValue() {
		return minValue;
	}
	public void setMinValue(Double minValue) {
		this.minValue = minValue;
	}
	public Double getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(Double maxValue) {
		this.maxValue = maxValue;
	}

	@Override
	public String toString() {
		return "Instrument [factorWeights=" + Arrays.toString(factorWeights) + ", minValue=" + minValue + ", maxValue="
				+ maxValue + "]";
	}

	
}
