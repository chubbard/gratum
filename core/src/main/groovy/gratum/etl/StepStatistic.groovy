package gratum.etl

import groovy.transform.CompileStatic

@CompileStatic
class StepStatistic {
    private String name
    private int loaded = 0
    private Map<RejectionCategory,Integer> rejections = [:]
    private long duration = 0

    StepStatistic(String name) {
        this.name = name
    }

    String getName() {
        return name
    }

    int getLoaded() {
        return loaded
    }

    int getTotalRejections() {
        return (Integer)rejections.values().sum(0)
    }

    Map<RejectionCategory, Integer> getRejections() {
        return rejections
    }

    long getDuration() {
        return duration
    }

    int incrementLoaded(int increment = 1) {
        this.loaded += increment
        return this.loaded
    }

    long incrementDuration(long duration) {
        this.duration += duration
        return this.duration
    }

    int incrementRejections(RejectionCategory rejectionCategory, int increment = 1) {
        rejections[rejectionCategory] = (rejections[rejectionCategory] ?: 0) + increment
    }

    double getAvgDuration() {
        Integer denominator = (loaded + ((Integer)rejections.values().sum()) )
        denominator > 0 ? duration / denominator.doubleValue() : 0.0d
    }
}
