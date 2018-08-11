package gratum.etl

import gratum.csv.CSVFile
import gratum.source.ChainedSource
import gratum.source.Source

import java.text.SimpleDateFormat

class Step {
    public String name
    public Closure step

    Step(String name, Closure step) {
        this.name = name
        this.step = step
    }
}

public class Pipeline implements Source {

    LoadStatistic statistic
    Source src
    List<Step> processChain = []
    List<Closure> doneChain = []
    Pipeline rejections
    boolean complete = false

    Pipeline(String name) {
        this.statistic = new LoadStatistic([filename: name])
    }

    Pipeline(LoadStatistic copy) {
        this.statistic = copy
    }

    public static create( String name, Closure startClosure ) {
        Pipeline pipeline = new Pipeline(name)
        pipeline.src = new Source() {
            @Override
            void start(Closure pipelineClosure) {
                startClosure( pipelineClosure )
            }
        }
        return pipeline
    }

    public getName() {
        return this.statistic.filename
    }

    public Pipeline addStep( String name = null, Closure<Map> step ) {
        step.delegate = this
        processChain << new Step( name, step )
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

    public Pipeline trim() {
        addStep("trim()") { Map row ->
            row.each { String key, Object value -> row[key] = (value as String).trim() }
            return row
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

    public Pipeline branch(Map<String,Object> condition, Closure<Void> split) {
        Pipeline branch = new Pipeline( name )
        split(branch)

        addStep( "branch(${condition})" ) { Map row ->
            if( matches( condition, row )) {
                branch.process( row )
            }
            return row
        }
    }

    public Pipeline join( Pipeline other, def columns, boolean left = false ) {
        Map<String,List<Map>> cache =[:]
        other.addStep("join(${other.name}, ${columns}).cache") { Map row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        return inject("join(${this.name}, ${columns})", true) { Map row ->
            if( !other.complete ) {
                other.go()
            }
            String key = keyOf( row, leftColumn(columns) )

            if( left ) {
                if( cache.containsKey(key) ) {
                    return cache[key].collect { Map k ->
                        Map j = k.clone()
                        j.putAll(row)
                        return j
                    }
                } else {
                    // make sure we add columns even if they are null so sources write out columns we expect.
                    if( !cache.isEmpty() ) {
                        String c = cache.keySet().first()
                        cache[c].first().each { String i, Object v ->
                            if( !row.containsKey(i) ) row[i] = null
                        }
                    }
                    return row
                }
            } else if( cache.containsKey(key) ) {
                return cache[key].collect { Map k ->
                    Map j = k.clone()
                    j.putAll(row)
                    return j
                }
            } else {
                reject("Could not join on ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
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

    public Pipeline groupBy( String... columns ) {
        Map cache = [:]
        addStep("groupBy(${columns.join(',')})") { Map row ->
            Map current = cache
            columns.eachWithIndex { String col, int i ->
                if( !current.containsKey(row[col]) ) {
                    if( i + 1 < columns.size() ) {
                        current[row[col]] = [:]
                        current = (Map)current[row[col]]
                    } else {
                        current[row[col]] = []
                    }
                } else if( i + 1 < columns.size() ) {
                    current = (Map)current[row[col]]
                }
            }

            current[ row[columns.last()] ] << row
            return row
        }

        Pipeline parent = this
        Pipeline other = new Pipeline( this.statistic )
        other.src = new Source() {
            @Override
            void start(Closure closure) {
                parent.start() // first start our parent pipeline
                closure( cache )
            }
        }
        return other
    }

    public Pipeline sort(String... columns) {
        // todo sort externally

        TreeSet<Map> ordered = new TreeSet<>(new Comparator<Map>() {
            @Override
            int compare(Map o1, Map o2) {
                for( String key : columns ) {
                    int value = o1[key] <=> o2[key]
                    if( value != 0 ) return value;
                }
                return 0
            }
        })

        addStep("sort(${columns})") { Map row ->
            ordered << row
            return row
        }

        Pipeline next = new Pipeline(statistic)
        next.src = new ChainedSource( this )
        after {
            ((ChainedSource)next.src).process( ordered )
        }

        return next
    }

    Pipeline asDouble(String column) {
        addStep("asDouble(${column})") { Map row ->
            String value = row[column] as String
            if( value ) row[column] = Double.parseDouble( value )
            return row
        }
    }

    Pipeline asInt(String column) {
        addStep("asInt(${column})") { Map row ->
            String value = row[column] as String
            if( value ) row[column] = Integer.parseInt(value)
            return row
        }
    }

    Pipeline asBoolean(String column) {
        addStep("asBoolean(${column}") { Map row ->
            String value = row[column]
            if( value ) {
                switch( value ) {
                    case "Y":
                    case "y":
                    case "yes":
                    case "YES":
                    case "Yes":
                    case "1":
                    case "T":
                    case "t":
                        row[column] = true
                        break
                    case "n":
                    case "N":
                    case "NO":
                    case "no":
                    case "No":
                    case "0":
                    case "F":
                    case "f":
                    case "null":
                    case "Null":
                    case "NULL":
                    case null:
                        row[column] = false
                        break
                    default:
                        row[column] = Boolean.parseBoolean(value)
                        break
                }
            }
            return row
        }
    }

    Pipeline asDate(String column, String format = "yyyy-MM-dd") {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format)
        addStep("asDate(${column}, ${format})") { Map row ->
            String val = row[column] as String
            if( val ) row[column] = dateFormat.parse( val )
            return row
        }
        return this
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

    public Pipeline printRow(String... columns) {
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

    public Pipeline addField(String fieldName, Closure fieldValue) {
        addStep("addField(${fieldName})") { Map row ->
            row[fieldName] = fieldValue(row)
            return row
        }
        return this
    }

    Pipeline unique(String column) {
        Set<Object> unique = [:] as HashSet
        addStep("unique(${column})") { Map row ->
            if( unique.contains(row[column]) ) return reject("Non-unique row returned", RejectionCategory.IGNORE_ROW)
            unique.add( row[column] )
            return row
        }
        return this
    }

    public Pipeline inject(String name, boolean reset, Closure closure) {
        Pipeline next = reset ? new Pipeline(name) : new Pipeline( statistic )
        next.src = new ChainedSource( this )

        addStep(name) { Map row ->
            Collection<Map> cc = closure( row )
            ((ChainedSource)next.src).process( cc )
            return row
        }
        return next
    }

    public Pipeline inject( Closure closure) {
        this.inject("inject()", false, closure )
    }

    public Pipeline inject( String name, Closure closure ) {
        this.inject( name, false, closure )
    }

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
            doneChain.each { Closure current ->
                current()
            }
        }
        complete = true
    }

    public LoadStatistic go(Closure closure = null) {
        start(closure)
        return statistic
    }

    public boolean process(Map row, int lineNumber = -1) {
        Map current = new LinkedHashMap(row)
        for (Step step : processChain) {
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
