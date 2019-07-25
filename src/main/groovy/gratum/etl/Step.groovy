package gratum.etl

import groovy.transform.CompileStatic

@CompileStatic
class Step {
    public String name
    public Closure step

    Step(String name, Closure step) {
        this.name = name
        this.step = step
    }
}
