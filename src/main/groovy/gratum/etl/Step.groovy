package gratum.etl

import gratum.csv.HaltPipelineException
import gratum.util.PipelineAbortException
import groovy.transform.CompileStatic

@CompileStatic
class Step {
    public static final int MAX_ERROR_THRESHOLD = 50

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
                incrementRejections( rejection.category )
                pipeline.doRejections(row, name, lineNumber)
            } else {
                loaded++
            }
            return next
        } catch(HaltPipelineException ex) {
            throw ex
        } catch(Exception ex) {
            Map<String,Object> next = handleExceptionToRejection(ex, lineNumber, row, pipeline)
            if( loaded == 0 && rejections[RejectionCategory.SCRIPT_ERROR]  > MAX_ERROR_THRESHOLD ) {
                throw new PipelineAbortException("${pipeline.name}:${name}:${lineNumber}: Halting pipeline due too many script errors encountered: ${ex}.", ex)
            } else {
                return next
            }
        } catch(Throwable t ) {
            handleExceptionToRejection(t, lineNumber, row, pipeline)
            throw new PipelineAbortException("${pipeline.name}:${name}:${lineNumber}: Halting pipeline due to a terminating exception encountered: ${t}.", t)
        } finally {
            long elapsed = System.currentTimeMillis() - start
            this.duration = this.duration + elapsed
        }
    }

    private Map<String,Object> handleExceptionToRejection(Throwable ex, int lineNumber, Map<String, Object> row, Pipeline pipeline) {
        incrementRejections(RejectionCategory.SCRIPT_ERROR)
        Rejection rejection = new Rejection("Encountered ${ex.message ?: ex} on ${pipeline.name} in step ${name} at ${lineNumber}", RejectionCategory.SCRIPT_ERROR, name)
        rejection.cause = ex
        row[Pipeline.REJECTED_KEY] = rejection
        pipeline.doRejections(row, name, lineNumber)
        return row
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

    void incrementRejections(RejectionCategory rejectionCategory) {
        rejections[rejectionCategory] = (rejections[rejectionCategory] ?: 0) + 1
    }
}
