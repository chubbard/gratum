package gratum.etl

import groovy.transform.CompileStatic

@CompileStatic
class AfterStep {

    StepStatistic statistic
    Closure<Void> callback

    AfterStep(String name, Closure<Void> callback) {
        this.statistic = new StepStatistic(name)
        this.callback = callback
    }

    public void execute() {
        long start = System.currentTimeMillis()
        callback.call()
        long end = System.currentTimeMillis()
        statistic.incrementDuration(end-start)
    }
}
