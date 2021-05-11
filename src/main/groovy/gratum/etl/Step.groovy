package gratum.etl

import groovy.transform.CompileStatic

@CompileStatic
class Step {
    public String name
    public Closure<Map<String,Object>> step
    private int loaded = 0
    private Map<RejectionCategory,Integer> rejections = [:]
    private long duration = 0

    Step(String name, Closure<Map<String,Object>> step) {
        this.name = name
        this.step = step
    }

    public Map<String,Object> execute(Pipeline pipeline, Map<String,Object> row, int lineNumber) {
        long start = System.currentTimeMillis()
        try {
            Map<String, Object> next = step.call(row)
            if (next == null || next[Pipeline.REJECTED_KEY]) {
                Rejection rejection = next[Pipeline.REJECTED_KEY] as Rejection
                rejections[rejection.category] = (rejections[rejection.category] ?: 0) + 1
                pipeline.doRejections(row, name, lineNumber)
            } else {
                loaded++
            }
            return next
        } finally {
            long elapsed = System.currentTimeMillis() - start
            this.duration = this.duration + elapsed
        }
    }

    public Map<RejectionCategory,Integer> getRejections() {
        return rejections
    }

    public long getDuration() {
        return duration
    }

    public int getLoaded() {
        return loaded
    }
}
