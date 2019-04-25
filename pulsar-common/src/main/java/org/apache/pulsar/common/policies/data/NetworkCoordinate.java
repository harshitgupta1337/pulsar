package org.apache.pulsar.common.policies.data;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.pulsar.policies.data.loadbalancer.JSONWritable;

import java.lang.IllegalArgumentException;

public class NetworkCoordinate {

    private boolean valid;    
    private double adjustment;
    private double error;
    private double height;
    private double[] coordinateVector;

    public NetworkCoordinate() {
        this.valid = false;
        this.adjustment = 0;
        this.error = 0;
        this.height = 0;
        this.coordinateVector = new double[8];
        for(int i = 0; i < coordinateVector.length; i++)
        {
            this.coordinateVector[i] = 0;
        }
    }

    public NetworkCoordinate(   boolean valid, 
                                double adjustment, 
                                double error, 
                                double height,
                                double[] coordinateVector) {
            this.valid = valid;
            this.adjustment = adjustment;
            this.error = error;
            this.height = height;
            if(coordinateVector.length != 8) { 
                throw new IllegalArgumentException("Needs to have 8 coordinates");
            }
            this.coordinateVector = coordinateVector;
    }

    public double getAdjustment() {
        return adjustment;
    }

    public void setAdjustment(double adjustment) {
        this.adjustment = adjustment;
    }

    public double getError() {
        return error;
    }

    public void setError(double error) {
        this.error = error;
    }

    public double getHeight() { 
        return height;
    }

    public void setHeight(double height) {
        this.height = height;
    }

    public double[] getCoordinateVector() {
        return coordinateVector;
    }

    public double getCoordinateAvg() {
        double total = 0;
        for(double coordinate : coordinateVector) {
            total += coordinate;
        }
        return (double) total/coordinateVector.length;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

}
