package gratum.etl

import groovy.transform.CompileStatic

/**
 * This object contains the statistics on how many items were processed by the Pipeline.  The name of the 
 * {@link gratum.etl.Pipeline} is contained in the name property.  Things included in this object are
 * number of rows loaded, number of rows rejected, categories and count of each row rejected, the total
 * time spent processing the {@link gratum.etl.Pipeline}, and the time each step took to process the rows.
 */
@CompileStatic
class LoadStatistic {
    String name
    List<StepStatistic> stepStatistics = []
    List<StepStatistic> doneStatistics = []
    Integer loaded = 0
    Long start = 0
    Long end = 0
    Long marker = 0

    public Duration getDuration() {
        return new Duration(end - start)
    }

    public long getElapsed() {
        return (System.currentTimeMillis() - marker)
    }

    public void mark() {
        marker = System.currentTimeMillis()
    }

    Long getStart() {
        return start
    }

    void setStart(Long start) {
        this.start = start
        mark()
    }

    Long getEnd() {
        return end
    }

    void setEnd(Long end) {
        this.end = end
    }

    public Integer getLoaded() {
        return loaded;
    }

    public Integer getRejections() {
        if( this.stepStatistics.rejections.isEmpty() ) return 0
        this.stepStatistics.rejections.inject(0) { sum, rej ->
            sum + (Integer)rej.values().sum(0)
        }
    }

    public Map<RejectionCategory, Map<String,Integer>> getRejectionsByCategory() {
        Map<RejectionCategory,Map<String,Integer>> results = [:]
        this.stepStatistics.inject(results) { combined, stepStatistic ->
            stepStatistic.rejections.each { cat, count ->
                if( !combined[cat] ) {
                    combined[cat] = [:] as Map<String,Integer>
                }
                combined[cat][stepStatistic.name] = count
            }
            combined
        }
    }

    public Integer getRejections(RejectionCategory category) {
        (Integer)this.stepStatistics.findResults { step -> step.rejections[category] }.sum()
    }

    public Integer getRejections(RejectionCategory cat, String step) {
        StepStatistic stat = this.stepStatistics.find { state -> state.name == step }
        return stat?.rejections[cat] ?: 0
    }

    public Map<String,Integer> getRejectionsFor( RejectionCategory category ) {
        return rejectionsByCategory[category]
    }

    public double avg( String step ) {
        return stepStatistics.find { s -> s.name == step }?.avgDuration
    }

    public String toString() {
        return toString(false)
    }
    
    public String toString(boolean stepDetails) {
        StringWriter out = new StringWriter()
        PrintWriter pw = new PrintWriter(out)
        if( stepDetails ) {
            pw.println("\n----")
            pw.println("Step Stats")
            pw.printf("| %-20s | %-10s | %-10s | %-17s | %-20s |%n", "Step Name", "Loaded", "Rejected", "Avg Duration (ms)", "Total Duration (ms)")
            pw.printf("-" * (20 + 10 + 10 + 17 + 20 + 16) + "%n")
            stepStatistics.each { stepStat ->
                pw.printf("| %-20s | %,10d | %,10d | %,17.2f | %,20d |%n", stepStat.name, stepStat.loaded, stepStat.totalRejections, stepStat.avgDuration, stepStat.duration )
            }
        }

        if( this.rejections > 0 ) {
            pw.println("\n----")
            pw.println("Rejections by category")
            this.rejectionsByCategory.each { RejectionCategory category, Map<String,Integer> steps ->
                pw.printf( "%s: %,d%n", category, steps.values().sum(0) )
                steps.each { String step, Integer count ->
                    pw.printf( "\t%s: %,d%n", step, count )
                }
            }
        }
        pw.println("\n----")
        pw.printf( "==> %s %nloaded %,d %nrejected %,d %ntook %s%n", this.name, this.loaded, this.rejections,this.duration )
        return out.toString()
    }

    void merge( LoadStatistic src ) {
        this.loaded += src.loaded
        src.stepStatistics.each { srcStat ->
            StepStatistic stepStat = stepStatistics.find { it.name == srcStat.name }
            if( stepStat ) {
                stepStat.incrementLoaded(srcStat.loaded)
                srcStat.rejections.each { category, count ->
                    stepStat.incrementRejections(category, count)
                }
                stepStat.incrementDuration( srcStat.duration )
            } else {
                stepStatistics << srcStat
            }
        }
    }

    boolean contains(String name) {
        stepStatistics.find { it.name == name } != null
    }

    @Deprecated
    Map<String,Long> getStepTimings() {
        stepStatistics.collectEntries() {step ->
            [step.name, step.duration]
        }
    }
}
