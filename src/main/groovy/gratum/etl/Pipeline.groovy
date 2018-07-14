package gratum.etl

import gratum.csv.CSVFile
import gratum.source.CsvSource
import gratum.source.Source

public class Pipeline implements Source {

    LoadStatistic statistic
    Source src
    List<Map<String,Object>> processChain = []
    List<Closure> doneChain = []
    Pipeline rejections

    Pipeline(String name) {
        this.statistic = new LoadStatistic([filename: name])
    }

    Pipeline(LoadStatistic copy) {
        this.statistic = copy
    }

    public getName() {
        return this.statistic.filename
    }

    public Pipeline addStep( String name = null, Closure<Map> step ) {
        processChain << [ name: name, step: step ]
        return this
    }

    public Pipeline after( Closure<Void> step ) {
        doneChain << step
        return this
    }

    public Pipeline onRejection( Closure<Void> branch ) {
        if( !rejections ) rejections = new Pipeline("Rejections(${name})")
        branch( rejections )
        return this
    }

    public Pipeline concat( Pipeline src ) {
        Pipeline original = this
        this.after {
            int line = 0
            src.addStep("concat(${src.name})") { Map row ->
                line++
                original.process( row, line )
            }.start()
        }
        return this
    }

    public Pipeline filter(Closure callback) {
        addStep( "filter()" ) { Map row ->
            return callback(row) ? row : reject("Row did not match the filter closure.", RejectionCategory.IGNORE_ROW)
        }
        return this
    }

    public Pipeline filter( Map columns ) {
        addStep( "filter ${ nameOf(columns) }" ) { Map row ->
            if(matches(columns, row)) {
                return row
            } else {
                return reject("Row did not match the filter ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
        return this
    }

    private boolean matches(Map columns, Map row) {
        return columns.keySet().inject(true) { match, key ->
            if( columns[key] instanceof Collection ) {
                match && ((Collection)columns[key]).contains( row[key] )
            } else {
                match && row[key] == columns[key]
            }
        }
    }

    public Pipeline branch( Closure<Void> split) {
        Pipeline branch = new Pipeline( name )

        split( branch )

        addStep( "branch()" ) { Map row ->
            branch.process( row )
            return row
        }
    }

    public Pipeline join( Pipeline other, def columns ) {
        Map<String,List<Map>> cache =[:]
        other.addStep("join(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        addStep("join(${this.name}, ${columns})") { Map row ->
            String key = keyOf( row, leftColumn(columns) )

            // todo how do we handle row multiplication when we want to add more than 1 item to the process chain,
            // right now this is going to k
            return cache.containsKey(key) ? row.putAll( cache[key].first() ) : null
        }

        return this
    }

    public Pipeline fillDownBy( Closure<Boolean> decider ) {
        Map previousRow = null
        addStep("fillDownBy()") { Map row ->
            if( previousRow && decider( row, previousRow ) ) {
                row.each { String col, Object value ->
                    // todo refactor valid_to out for excluded
                    if (col != "valid_To" && (value == null || value.isEmpty())) {
                        row[col] = previousRow[col]
                    }
                }
            }
            previousRow = row.clone()
            return row
        }
        return this
    }

    public Pipeline renameFields( Map fieldNames ) {
        addStep("renameFields(${fieldNames}") { Map row ->
            for( String src : fieldNames.keySet() ) {
                String dest = fieldNames.get( src )
                row[dest] = row.remove( src )
            }
            return row
        }
        return this
    }

    public Pipeline intersect( Pipeline other, def columns ) {
        Map <String,List<Map>> cache = [:]
        other.addStep("intersect(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }.start()

        addStep("intersect(${this.name}, ${columns})") { Map row ->
            String key = keyOf( row, leftColumn(columns) )
            row.included = cache.containsKey(key)
            return row
//            return cache.containsKey(key) ? row : null
        }

        return this
    }

    private List<String> leftColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).keySet().asList()
        } else {
            return [columns.toString()]
        }
    }

    private List<String> rightColumn(def columns) {
        if( columns instanceof Collection ) {
            return ((Collection)columns).toList()
        } else if( columns instanceof Map ) {
            return ((Map)columns).values().asList()
        } else {
            return [columns.toString()]
        }
    }

    private String nameOf(Map columns) {
        return columns.keySet().collect() { key -> key + "->" + columns[key] }.join(',')
    }

    public Pipeline groupBy( List columns, boolean add = false ) {
        Map cache = [:]
        addStep("groupBy(${columns.join(',')})") { Map row ->
            String key = keyOf(row, columns)

            if( !cache.containsKey(key) ) {
                cache[key] = []
            }
            cache[key] << row
            return row
        }

        Pipeline parent = this
        Pipeline other = new Pipeline( this.name )
        other.src = new Source() {
            @Override
            void start(Closure closure) {
                parent.start() // first start our parent pipeline
                cache.values().sort { a, b -> b.size() <=> a.size() }.each { List<Map<String,String>> rows ->
                    Map r = columns.inject([:]) { acc, col ->
                        acc[col] = rows.first()[col]
                        return acc
                    }

                    r["count"] = rows.size()

                    if( add ) {
                        rows.eachWithIndex { Map<String,String> current, int i ->
                            current.each { col, v ->
                                if( !columns.contains(col) ) r["${col}_${i+1}"] = v
                            }
                        }
                    }
                    closure(r)
                }
            }
        }
        return other
    }

    public Pipeline save( String filename, String separator = ",", List<String> columns = null ) {
        CSVFile out = new CSVFile( filename, separator )
        addStep("Save to ${out.file.name}") { Map row ->
            if( columns ) {
                out.write( row, columns?.toArray(new String[columns.size()]) )
            } else {
                out.write( row )
            }
            return row
        }

        after {
            out.close()
        }
        return this
    }

    public Pipeline print(String... columns) {
        addStep("print()") { Map row ->
            if( columns ) {
                println( "[ ${columns.toList().collect { row[it] }.join(',')} ]" )
            } else {
                println( row )
            }
            return row
        }
        return this
    }

    public Pipeline progress( int col = 50 ) {
        int line = 1
        addStep("progress()") { Map row ->
            line++
            printf(".")
            if( line % col ) println()
            row
        }
    }

    public Pipeline printStatistics( boolean timings = false ) {
        after {
            if( timings ) {
                println("\n----")
                println("Step Timings")
                statistic.stepTimings.each { String step, Long totalTime ->
                    printf("%s: %,.2f ms%n", step, statistic.avg(step) )
                }
            }

            if( statistic.rejections > 0 ) {
                println("\n----")
                println("Rejections by category")
                statistic.rejectionsByCategory.each { RejectionCategory category, Integer count ->
                    printf( "%s: %,d%n", category, count )
                }
            }
            println("\n----")
            printf( "==> %s loaded %,d rejected %,d and took %,d ms%n", statistic.filename, statistic.loaded, statistic.rejections,statistic.elapsed )
        }
    }

    public Pipeline addField(String fieldName, Closure fieldValue) {
        addStep("addField(${fieldName})") { Map row ->
            row[fieldName] = fieldValue(row)
            return row
        }
        return this
    }

    @Override
    public void start(Closure closure = null) {
        if( closure ) addStep("tail", closure)

        statistic.start = System.currentTimeMillis()
        int line = 1
        src.start { Map row ->
            line++
            return process(row, line)
        }
        statistic.end = System.currentTimeMillis()

        statistic.timed("Done Callbacks") {
            doneChain.each { Closure c ->
                c()
            }
        }
    }

    public LoadStatistic go(Closure closure = null) {
        start(closure)
        return statistic
    }

    public boolean process(Map row, int lineNumber = -1) {
        Map current = new LinkedHashMap(row)
        for (Map step : processChain) {
            try {
                boolean stop = statistic.timed(step.name) {
                    def ret = step.step(current)
                    if (!ret || ret instanceof Rejection ) {
                        Rejection rejection = ret ? (Rejection)ret : new Rejection("Unknown reason", RejectionCategory.REJECTION)
                        rejection.step = step.name
                        current.rejectionCategory = rejection.category
                        current.rejectionReason = rejection.reason
                        current.rejectionStep = rejection.step
                        statistic.reject( rejection?.category ?: RejectionCategory.REJECTION)
                        rejections?.process( current, lineNumber )
                        return true
                    }
                    current = ret
                    return false
                }
                if (stop) return false
            } catch (Exception ex) {
                throw new RuntimeException("Line ${lineNumber > 0 ? lineNumber : row}: Error encountered in step ${statistic.filename}.${step.name}", ex)
            }
        }
        statistic.loaded++
        return false // don't stop!
    }

    private String keyOf( Map row, List<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

    public static Pipeline csv( String filename, String separator = "," ) {
        Pipeline pipeline = new Pipeline( filename )
        pipeline.src = new CsvSource( new File(filename), separator )
        return pipeline
    }

    public static Rejection reject( String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        return new Rejection( reason, category )
    }

    public static void main(String[] args) {
        CliBuilder cli = new CliBuilder(usage: 'Pipeline [options] script', header: 'Options:')
        cli.P(argName: 'property', 'Set a System property for scripts', args: 1)
        cli.h(argName: 'help', 'Print this help message', args: 0)

        def options = cli.parse(args)

        if( options.h ) {
            cli.usage()
            System.exit(-1)
        }

        List<String> scripts = options.arguments()
        if( scripts.isEmpty() ) {
            println("Missing a script to run.")
            cli.usage()
            System.exit(-1)
        }

        GroovyShell shell = new GroovyShell()
        scripts.each { String script ->
            println("Evaulating ${script}...")
            shell.evaluate(new File(script))
        }
    }
}
