package gratum.etl

import groovy.transform.CompileStatic

@CompileStatic
class Step {
    public String name
    public Closure<Map<String,Object>> step

    Step(String name, Closure<Map<String,Object>> step) {
        this.name = name
        this.step = step
    }
}
