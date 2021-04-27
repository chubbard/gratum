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
    Map<RejectionCategory, Map<String,Integer>> rejectionsByCategory = [:]
    Map<String,Long> stepTimings = [:]
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
        return rejectionsByCategory.inject(0) { Integer sum, RejectionCategory cat, Map<String,Integer> stepCounts ->
            sum + (Integer)stepCounts.values().sum()
        }
    }

    public void reject(Rejection rejection) {
        RejectionCategory category = rejection?.category ?: RejectionCategory.REJECTION
        if( !rejectionsByCategory[category] ) {
            rejectionsByCategory[category] = [(rejection.step): 0]
        } else if( !rejectionsByCategory[category][rejection.step] ) {
            rejectionsByCategory[category][rejection.step] = 0
        }
        Integer count = rejectionsByCategory[category][rejection.step] + 1
        rejectionsByCategory[category][rejection.step] = count
    }

    public Map<RejectionCategory, Map<String,Integer>> getRejectionsByCategory() {
        return rejectionsByCategory
    }

    public Integer getRejections(RejectionCategory category) {
        return (Integer)rejectionsByCategory.get(category).values().sum(0)
    }

    public Integer getRejections(RejectionCategory cat, String step) {
        return rejectionsByCategory?.get(cat)?.get(step)
    }

    public Map<String,Integer> getRejectionsFor( RejectionCategory category ) {
        return rejectionsByCategory[category];
    }

    public <R> R timed( String stepName, Closure<R> c ) {
        if( !stepTimings.containsKey(stepName) ) stepTimings.put( stepName, 0L )
        long start = System.currentTimeMillis()
        R ret = c.call()
        long duration = System.currentTimeMillis() - start
        stepTimings[stepName] = stepTimings[stepName] + duration
        return ret
    }

    public double avg( String step ) {
        double avg = stepTimings[step] / (loaded + rejections)
        return avg
    }

    public String toString() {
        return toString(false)
    }
    
    public String toString(boolean timings) {
        StringWriter out = new StringWriter()
        PrintWriter pw = new PrintWriter(out)
        if( timings ) {
            pw.println("\n----")
            pw.println("Step Timings")
            this.stepTimings.each { String step, Long totalTime ->
                pw.printf("%s: %,.2f ms%n", step, this.avg(step) )
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
        pw.printf( "==> %s %nloaded %,d %nrejected %,d %ntook %,d ms%n", this.name, this.loaded, this.rejections,this.elapsed )
        return out.toString()
    }
}
