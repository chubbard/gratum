package gratum.etl

class AfterStep {

    long duration = 0L
    Closure<Void> callback

    AfterStep(Closure<Void> callback) {
        this.callback = callback
    }

    public void execute() {
        long start = System.currentTimeMillis()
        callback.call()
        long end = System.currentTimeMillis()
        duration = (end-start)
    }
}
