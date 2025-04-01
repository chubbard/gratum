package gratum.etl

import gratum.csv.HaltPipelineException
import gratum.util.PipelineAbortException
import groovy.transform.CompileStatic

@CompileStatic
class Step {
    public static final int MAX_ERROR_THRESHOLD = 50

    private StepStatistic statistics
    public Closure<Map<String,Object>> step
    private Pipeline pipeline

    Step(Pipeline pipeline, String name, Closure<Map<String,Object>> step) {
        this.statistics = new StepStatistic(name)
        this.step = step
        this.pipeline = pipeline
    }

    public Map<String,Object> execute(Map<String,Object> row, int lineNumber) {
        long start = System.currentTimeMillis()
        try {
            Map<String, Object> next = step.call(row)
            if (next == null || next[Pipeline.REJECTED_KEY]) {
                doRejections(next, lineNumber)
            } else {
                statistics.incrementLoaded()
            }
            return next
        } catch(HaltPipelineException ex) {
            throw ex
        } catch(Exception ex) {
            Map<String,Object> next = handleExceptionToRejection(ex, lineNumber, row)
            if( loaded == 0 && statistics.rejections[RejectionCategory.SCRIPT_ERROR]  > MAX_ERROR_THRESHOLD ) {
                throw new PipelineAbortException("${pipeline.name}:${this.statistics.name}:${lineNumber}: Halting pipeline due too many script errors encountered: ${ex}.", ex)
            } else {
                return next
            }
        } catch(Throwable t ) {
            handleExceptionToRejection(t, lineNumber, row)
            throw new PipelineAbortException("${pipeline.name}:${this.statistics.name}:${lineNumber}: Halting pipeline due to a terminating exception encountered: ${t}.", t)
        } finally {
            long elapsed = System.currentTimeMillis() - start
            this.statistics.incrementDuration(elapsed)
        }
    }

    private void doRejections(Map<String, Object> next, int lineNumber) {
        Rejection rejection = next[Pipeline.REJECTED_KEY] as Rejection
        statistics.incrementRejections(rejection.category)
        pipeline.doRejections(next, statistics.name, lineNumber)
    }

    private Map<String,Object> handleExceptionToRejection(Throwable ex, int lineNumber, Map<String, Object> row) {
        statistics.incrementRejections(RejectionCategory.SCRIPT_ERROR)
        Rejection rejection = new Rejection(
                "Encountered ${ex.message ?: ex} on ${pipeline.name} in step ${this.statistics.name} at ${lineNumber}",
                RejectionCategory.SCRIPT_ERROR,
                statistics.name,
                ex)
        row[Pipeline.REJECTED_KEY] = rejection
        pipeline.doRejections(row, statistics.name, lineNumber)
        return row
    }

    Map<RejectionCategory,Integer> getRejections() {
        return statistics.rejections
    }

    long getDuration() {
        return statistics.duration
    }

    int getLoaded() {
        return statistics.loaded
    }

    StepStatistic getStatistics() {
        return this.statistics
    }

    Map<String,Object> reject(Map<String,Object> row, String reason, RejectionCategory category = RejectionCategory.REJECTION, Throwable ex = null) {
        row[Pipeline.REJECTED_KEY] = new Rejection(reason, category, statistics.name, ex )
        return row
    }
}
