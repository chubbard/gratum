package gratum.etl

import gratum.csv.CSVFile
import gratum.pgp.PgpContext
import gratum.sink.CsvSink
import gratum.sink.JsonSink
import gratum.sink.Sink
import gratum.source.AbstractSource
import gratum.csv.HaltPipelineException
import gratum.source.ChainedSource
import gratum.source.ClosureSource
import gratum.source.CollectionSource
import gratum.source.Source
import groovy.transform.CompileDynamic
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.regex.Pattern

/**
 * A Pipeline represents a series of steps that will be performed on 1 or more rows.  Rows are Map objects
 * that are passed from the Source through each step individually.  A row can be modified, transformed into 
 * something else, or rejected by a step.  If a row is rejected in any step all successive steps are not 
 * processed and the row is passed to the rejections Pipeline (if one exists).  Any rows that pass through 
 * the entire Pipeline are said to have been loaded.  Any row that does not pass through all steps is said 
 * to be rejected.
 * 
 * Rows originate from the Pipeline's {@link gratum.source.Source}.  The {@link gratum.source.Source} sends
 * rows into the Pipeline until it finishes.  The Pipeline keeps a set of statistics about how many rows
 * were loaded, rejected, the types of rejections, timing, etc.  This is kept in the {@link gratum.etl.LoadStatistic}
 * instance and returned from the {@link Pipeline#go()} method.
 *
 * Example:
 *
 * <pre>
 *      LoadStatistic stats = http("http://api.open-notify.org/astros.json").
 *          inject { json -&gt;
 *              json.people
 *          }.
 *          printRow().
 *          go()
 * </pre>
 *
 * In the above example you can see it using an {@link gratum.source.HttpSource} to fetch JSON data.  
 * That data is returned as a Map object which has other nested objects within it.  In this case it's
 * pulling out the "people" column which is a Collection of people objects.  Then it injects those members
 * into the down stream steps which uses printRow to print it to the console.  The output would look like
 * the following:
 *
 * <pre>
 *    [name:Sergey Prokopyev, craft:ISS]
 *    [name:Alexander Gerst, craft:ISS]
 *    [name:Serena Aunon-Chancellor, craft:ISS]
 *    ===&gt;
 *    ----
 *    ==&gt; inject()
 *    loaded 3
 *    rejected 0
 *    took 1 ms
 * </pre>
 */
@CompileStatic
public class Pipeline {

    public static final String REJECTED_KEY = "__reject__"
    public static final int DO_NOT_TRACK = -1
    public static final Logger logger = LoggerFactory.getLogger(Pipeline)

    CharSequence name
    Source src
    List<Step> processChain = []
    List<AfterStep> doneChain = []
    Pipeline parent
    Pipeline rejections
    boolean complete = false
    int loaded = 0

    Pipeline(CharSequence name, Pipeline parent = null) {
        this.name = name
        this.parent = parent
    }

    /**
     * Creates a pipeline where startClosure is the source.  The startClosure is passed another closure that it can use
     * to pass an individual row to the Pipeline.
     *
     * @param name name of the pipeline to create
     * @param startClosure A closure that is called with a closure.  The startClosure can call the closure argument it's 
     * passed to send a row into the pipeline.
     * @return The pipeline attached to results of the startClosure.
     */
    public static Pipeline create( CharSequence name,
                                   @DelegatesTo(Pipeline)
                                   @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                                   Closure startClosure ) {
        ClosureSource.of( startClosure ).name( name ).into()
    }

    /**
     * The name of the pipeline.  This is used by {@link LoadStatistic} to identify what Pipeline it came from.
     *
     * @return The name of the Pipeline
     */
    public CharSequence getName() {
        return this.name
    }

    /**
     * Prepend a step to the pipeline.
     * @param name The Step name
     * @param step The code used to process each row on the Pipeline.
     * @return this Pipeline.
     */
    public Pipeline prependStep( CharSequence name = null,
                                 @DelegatesTo(Pipeline)
                                 @ClosureParams(value = FromString, options = ["java.lang.Map<String,String>"])
                                 Closure<Map<String,Object>> step ) {
        step.delegate = this
        processChain.add(0, new Step( name, step ) )
        return this
    }

    /**
     * Adds a step to the pipeline.  It's passed an optional name to identify the step by, and a closure that represents
     * the individual step.  It returns the Map to be processed by the next step in the pipeline, typically it simply returns the same
     * row it was passed.  If it returns null or {@link Rejection} then it will reject this row, stop processing additional
     * steps, and pass the current row to the rejections pipeline.
     *
     * @param name The step name
     * @param step The code used to process each row on the Pipeline.
     * @return this Pipeline.
     */
    public Pipeline addStep( CharSequence name = null,
                             @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                             @DelegatesTo(Pipeline) Closure<Map<String,Object>> step ) {
        step.delegate = this
        processChain << new Step( name, step )
        return this
    }

    /**
     * Adds a closure to the end of the Pipeline.  This is called after all rows are processed.  This closure is
     * invoked without any arguments.
     *
     * @param step the Closure that is invoked after all rows have been processed.
     * @return this Pipeline.
     */
    public Pipeline after( @DelegatesTo(Pipeline) Closure<Void> step ) {
        step.delegate = this
        doneChain << new AfterStep(step)
        return this
    }

    /**
     * Takes a closure that is passed the rejection Pipeline.  The closure can register steps on the rejection
     * pipeline, and any rejections from the parent pipeline will be passed through the given rejection pipeline.
     *
     * @param configure Closure that's passed the rejection the pipeline
     * @return this Pipeline
     */
    public Pipeline onRejection( @DelegatesTo(Pipeline)
                                 @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                                 Closure<Pipeline> configure ) {
        if( parent ) {
            parent.onRejection( configure )
        } else {
            if( !rejections ) {
                rejections = new Pipeline("Rejections(${name})")
                rejections.addStep("Remap rejections to columns") { row ->
                    Map<String,Object> current = (Map<String,Object>)row.clone()
                    Rejection rejection = (Rejection)current.remove(REJECTED_KEY)
                    if( rejection ) {
                        current.rejectionCategory = rejection.category
                        current.rejectionReason = rejection.reason
                        current.rejectionStep = rejection.step
                        current.rejectionException = !rejection.throwable ? "" : new StringWriter().with {
                            rejection.throwable.printStackTrace(new PrintWriter(it, true))
                            it.toString()
                        }
                    } else {
                        logger.warn("Rejection was missing during processing rejections.")
                    }
                    return current
                }
                after {
                    rejections.finished()
                    return
                }
            }
            configure.delegate = rejections
            configure( rejections )
        }
        return this
    }

    /**
     * Concatenates the rows from this pipeline and the given pipeline.  The resulting Pipeline will process all
     * rows from this pipeline and the src pipeline.
     *
     * @param src The pipeline
     * @return Returns a new pipeline that combines all of the rows from this pipeline and the src pipeline.
     */
    public Pipeline concat( Pipeline src ) {
        Pipeline concatPipe = createChildPipeline("concat(${src.name})")
        int line = 0
        src.addStep("concat(${src.name})"){ row ->
            line++
            concatPipe.process(row, line)
            return row
        }
        this.after {
            src.start()
        }
        return concatPipe
    }

    private Pipeline createChildPipeline(String name) {
        Pipeline child = new Pipeline(name, this).source(new ChainedSource(this))
        int line = 0
        addStep(name) { row ->
            line++
            child.process(row, line)
            return row
        }
        return child
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the given closure returns true to the next step
     * on the pipeline.  All rows where the closure returns false are rejected.
     *
     * @param callback A callback that is passed a row, and returns a boolean.  All rows that return a false are rejected.
     * @return A Pipeline that contains only the rows that matched the filter.
     */
    public Pipeline filter(CharSequence name = "filter()",
                           @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                           @DelegatesTo(Pipeline) Closure callback) {
        callback.delegate = this
        addStep( name ) { row ->
            if( !callback(row) ) {
                return reject(row,"Row did not match the filter closure.", RejectionCategory.IGNORE_ROW )
            }
            return row
        }
        return this
    }

    /**
     * This adds a step to the Pipeline that passes all rows where the values of the columns on the given Map are equal
     * to the columns in the row.  This is a boolean AND between columns.  For example:
     *
     * .filter( [ hair: 'Brown', eyeColor: 'Blue' ] )
     *
     * In this example all rows where hair = Brown AND eyeColor = Blue are passed through the filter.
     *
     * You can also pass a java.util.Collection of values to compare a column to.  For example,
     *
     * .filter( [hair: ['Brown','Blonde'], eyeColor: ['Blue','Green'] ] )
     *
     * In this example you need hair of 'Brown' or 'Blonde' and eyeColor of 'Blue' or 'Green'.
     *
     * You can also pass a groovy.lang.Closure as a value for a column and it will filter based
     * on the value returned by the Closure.  For example,
     *
     * .filter( [hair: { String v -> v.startsWith('B') }, eyeColor: 'Brown'] )
     *
     * In this example it invokes the Closure with the value at row['hair'] and the Closure evaluates
     * to a boolean to decide if a row is filtered or not.
     *
     * .filter( "*": { Map row -> row['hair'] == "Brown" || row['eyeColor'] == 'Blue' } )
     *
     * This is the wildcard example, that allows you to embed a closure into the filter Map to create
     * much more complex queries using the entire row.  So implementing OR logic is possible between
     * multiple fields.
     *
     * @param columns a Map that contains the columns, and their values that are passed through
     * @return A pipeline that only includes the rows matching the given filter.
     */
    public Pipeline filter( Map columns ) {
        Condition condition = new Condition( columns )
        addStep( "filter ${ condition }" ) { row ->
            if( condition.matches(row) ) {
                return row
            } else {
                return reject( row,"Row did not match the filter ${columns}", RejectionCategory.IGNORE_ROW )
            }
        }
        return this
    }

    /**
     * Returns a Pipeline where all white space is removed from all columns contained within the rows.
     *
     * @return Pipeline where all columns of each row has white space removed.
     */
    public Pipeline trim() {
        addStep("trim()") { row ->
            row.each { String key, Object value -> row[key] = (value as String)?.trim() }
            return row
        }
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline that is passed to the given closure.  The given closure
     * can configure additional steps on the branched Pipeline.  The rows passed through this Pipeline are not modified.
     *
     * @param split The closure that is passed a new Pipeline where all the rows from this Pipeline are copied onto.
     * @return this Pipeline
     */
    public Pipeline branch( CharSequence branchName = "branch",
                            @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                            Closure<Pipeline> split) {
        final Pipeline branch = new Pipeline( "${name}/${branchName}" )

        Pipeline tail = split( branch )

        addStep( "branch(${branchName})" ) { row ->
            branch.process( new LinkedHashMap(row) )
            return row
        }

        after {
            tail.parent?.finished()
            tail.finished()
        }
    }

    /**
     * Copies all rows on this Pipeline to another Pipeline where the given condition returns is true.  The given
     * condition works the same way {@link #filter(java.util.Map)} does.  This ia combination of branch and filter.
     * No rows on this pipeline are filtered out.  Only the rows on the branch will be filtered.
     *
     * @param condition The conditions that must be equal in order for the row to be copied to the branch.
     * @param split The closure that is passed the branch Pipeline.
     * @return this Pipeline
     */
    public Pipeline branch(Map<String,?> condition,
                           @DelegatesTo(Pipeline)
                           @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                           Closure<Pipeline> split) {
        Pipeline branch = new Pipeline( "${name}/branch(${condition})" )
        Pipeline tail = split(branch)

        Condition selection = new Condition( condition )
        addStep( "branch(${condition})" ) { row ->
            if( selection.matches( row )) {
                branch.process( new LinkedHashMap(row) )
            }
            return row
        }

        after {
            tail.parent?.finished()
            tail.finished()
        }
    }

    /**
     * Returns Pipeline that joins the columns from this Pipeline with the given Pipeline where the columns are
     * equal.  It will perform a left or right join depending on the left parameter.  A left join will return the
     * row even if it doesn't find a match row on the right (also known as a left join).  A right join (ie left = false) will not return a
     * row if it doesn't find a matching row on the right (Also known as an inner join).  Default is left = false.
     *
     * Columns can be specified in 3 different ways: Map, Collection or Object.  Using a Map allows you to specify both
     * keys from the left Pipeline and the right Pipeline thus mapping column to column.  For example, a Pipeline that
     * has People objects on it might have an id column, and the right Pipeline has Hobbies which carry a person_id column.
     * To Map id -> person_id you'd use the following:
     *
     * people.join( hobbies, [id: "person_id"] ]
     *
     * That would mean match up rows such that people[id] = hobbies[person_id].  Using a Collection simply means the columns
     * are the same name in both Pipelines.  And using an Object simply calls toString() on it and uses that as the column
     * name shared by both Pipelines.
     *
     * @param other The right side Pipeline to use for the join
     * @param columns The columns to join on
     * @param left perform a left join (ie true) or a right join (false)
     * @return A Pipeline where the rows contain all columns from the this Pipeline and right Pipeline joined on the given columns.
     */
    public Pipeline join( Pipeline other, def columns, boolean left = false ) {
        Map<String,List<Map<String,Object>>> cache =[:]
        other.addStep("join(${other.name}, ${columns}).cache") { row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }

        return this.inject("join(${this.name}, ${columns})", { Map<String,Object> row ->
            if( !other.complete ) {
                other.go()
            }
            String key = keyOf( row, leftColumn(columns) )

            if( left ) {
                if( cache.containsKey(key) ) {
                    return cache[key].collect { Map k ->
                        Map j = (Map)k.clone()
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
                    return [row]
                }
            } else if( cache.containsKey(key) ) {
                return cache[key].collect { Map k ->
                    Map j = (Map)k.clone()
                    j.putAll(row)
                    return j
                }
            } else {
                return [ reject( row,"Could not join on ${columns}", RejectionCategory.IGNORE_ROW ) ]
            }
        } as Closure<Iterable<Map<String,Object>>>)
    }

    /**
     * This returns a Pipeline where the rows with empty columns are filled in using the values in the previous row depending on
     * what the given closure returns.  If the closure returns true then any empty column (value == null or value.isEmpty()) will
     * be populated by the values in the previous row.
     *
     * @param decider a Closure which decides if the values from a prior row will be used to fill in missing values in the current row.
     * @return A Pipeline where the row's empty column values are filled in by the previous row.
     */
    public Pipeline fillDownBy( @DelegatesTo(Pipeline)
                                @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>", "java.util.Map<String,Object>"])
                                Closure<Boolean> decider ) {
        Map<String,Object> previousRow = null
        decider.delegate = this
        addStep("fillDownBy()") { row ->
            if( previousRow && decider( row, previousRow ) ) {
                row.each { String col, Object value ->
                    // todo refactor valid_to out for excluded
                    if (col != "valid_To" && (value == null || !value)) {
                        row[col] = previousRow[col]
                    }
                }
            }
            previousRow = (Map<String,Object>)row.clone()
            return row
        }
        return this
    }

    /**
     * Rename a row's columns in the given map to the value of the corresponding key.
     *
     * @param fieldNames The Map of src column to renamed names.
     * @return A Pipeline where all of the columns in the keys of the Map are renamed to the Map's corresponding values.
     */
    public Pipeline renameFields( Map fieldNames ) {
        addStep("renameFields(${fieldNames}") { row ->
            for( String src : fieldNames.keySet() ) {
                String dest = fieldNames.get( src )
                row[dest] = row.remove( src )
            }
            return row
        }
        return this
    }

    /**
     * Return a Pipeline where all of the rows from this Pipeline and adds a single column
     * "included" with a true/false value depending on whether the current row is occurs
     * in the given Pipeline and the values of the specified columns are equal in both 
     * Pipelines.
     *
     * @param other Pipeline to verify if the rows where the columns of those rows are equal 
     * to the rows in this Pipeline
     * @param the list of columns to check against (either Map or Collection).
     * return A Pipeline where each row in this Pipeline is in the new Pipeline and a colun 
     * included is added based on if this row according to the given columns also occurs in 
     * the other Pipeline
     */

    public Pipeline intersect( Pipeline other, def columns ) {
        Map <String,List<Map>> cache = [:]
        other.addStep("intersect(${other.name}, ${columns}).cache") { row ->
            String key = keyOf(row, rightColumn(columns) )
            if( !cache.containsKey(key) ) cache.put(key, [])
            cache[key] << row
            return row
        }.start()

        addStep("intersect(${this.name}, ${columns})") { row ->
            String key = keyOf( row, leftColumn(columns) )
            row.included = cache.containsKey(key)
            return row
//            return cache.containsKey(key) ? row : null
        }

        return this
    }

    private Iterable<String> leftColumn(Object columns) {
        if( columns instanceof Collection ) {
            return ((Collection<String>)columns)
        } else if( columns instanceof Map ) {
            return ((Map<String,String>)columns).keySet()
        } else {
            return [columns.toString()]
        }
    }

    private Iterable<String> rightColumn(Object columns) {
        if( columns instanceof Collection ) {
            return ((Collection<String>)columns)
        } else if( columns instanceof Map ) {
            return ((Map<String,String>)columns).values()
        } else {
            return [columns.toString()]
        }
    }

    /**
     * Return a Pipeline where the row is grouped by the given columns.  The resulting Pipeline will only
     * return a single row where the keys of that row will be the first column passed to the groupBy() method.
     * All other columns given will occur under the respective keys.  This yields a tree like structure where
     * the height of the tree is equal to the columns.length.  In the leaves of the tree are the rows that
     * matched all of their parents.
     *
     * @param columns The columns to group each row by.
     * @return A Pipeline that yields a single row that represents the tree grouped by the given columns.
     */
    public Pipeline groupBy( String... columns ) {
        Map cache = [:]
        addStep("groupBy(${columns.join(',')})") { row ->
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

            ((List)current[ row[columns.last()] ]) << row
            return row
        }

        Pipeline other = new Pipeline( name, this ).source(new AbstractSource() {
            @Override
            void doStart(Pipeline pipeline) {
                pipeline.parent.start() // first start our parent pipeline
                pipeline.process( cache, 1 )
            }
        })
        return other
    }

    /**
     * Return a Pipeline where the rows are ordered by the given columns and sort order.
     * @param columns a list of Tuple2 where the first item is the column name and the second is the sort order.
     * @return a Pipeline whose rows are sorted according to the given columns and order.
     */
    public Pipeline sort(Tuple2<String,SortOrder>... columns) {
        sort("sort(${columns.collect { t -> "${t.first}/${t.second}" }.join(",")}") {
            orderBy(columns)
        }
    }

    /**
     * Sort the rows according to the given comparator
     * @param name - The name identifying the step added to the pipeline for sort.
     * @param comparator - The comparator used to sort the rows.
     * @param configure - a closure to configure the behavior of the sort.  The delegate is a
     * {@link gratum.etl.SortConfig}
     * @return A Pipeline that orders the rows according to the Comparator
     */
    public Pipeline sort(CharSequence name, @DelegatesTo(SortConfig) Closure configure) {
        SortConfig cfg = new SortConfig()
        if( configure ) {
            configure.delegate = cfg
            configure()
        }
        File tmpDir = File.createTempDir("sorting_")
        List<Map> page = []
        int pageIndex = 1

        addStep(name) { row ->
            page << row
            if( page.size() >= cfg.pageSize && cfg.pageSize > 0 ) {
                page.sort(cfg.comparator)
                String filename = "${tmpDir}/page_${pageIndex++}.csv"
                CollectionSource.from(page).save(filename).go()
                page.clear()
            }
            return row
        }

        Pipeline next = new Pipeline(name, this).source(new ChainedSource(this))
        after {
            if( cfg.pageSize > 0 ) {
                // any residual rows left in the page buffer flush to disk
                if( !page.isEmpty() ) {
                    page.sort(cfg.comparator)
                    String filename = "${tmpDir}/page_${pageIndex++}.csv"
                    CollectionSource.from(page).save(filename).go()
                    page.clear()
                }

                List<File> pages = tmpDir.listFiles() as List<File>
                while(pages.size() > 1) {
                    File page1 = pages.pop()
                    File page2 = pages.pop()
                    CSVFile mergedPage = mergePage( new CSVFile(page1, ","), new CSVFile(page2, ","), cfg.comparator )
                    pages.add( mergedPage.file )
                    page1.delete()
                    page2.delete()
                }
                cfg?.after?.call(pages.first())
                if( cfg.downstream ) {
                    CSVFile csvFile = new CSVFile( pages.first(), "," )
                    Iterator<Map<String,Object>> rows = csvFile.mapIterator()
                    while( rows.hasNext() ) {
                        next.process( rows.next() )
                    }
                }
            } else {
                page.sort(cfg.comparator)
                ((ChainedSource)next.src).process( page )
            }
        }
        return next
    }

    /**
     * Return a Pipeline where the rows are ordered by the given columns.  The value of
     * each column is compared using the <=> operator.
     * @param columns to sort by
     * @return a Pipeline that where it's rows are ordered according to the given columns.
     */
    public Pipeline sort(String... columns) {
        sort("sort(${columns})" ) {
            orderBy(columns)
        }
    }

    CSVFile mergePage(CSVFile page1, CSVFile page2, Comparator<Map<String,Object>> comparator) {
        String[] p1 = page1.file.name.split("[_.]")
        String[] p2 = page2.file.name.split("[_.]")
        String v1 = p1[1]
        String v2 = p2[p2.length-2]
        CSVFile result = new CSVFile( new File(page1.file.parentFile, "page_${v1}_${v2}.csv"), "," )

        try {
            Iterator<Map<String, Object>> it1 = page1.mapIterator()
            Iterator<Map<String, Object>> it2 = page2.mapIterator()
            Map<String, Object> row1 = null
            Map<String, Object> row2 = null
            while (it1.hasNext() || it2.hasNext()) {
                if (!row1 && it1.hasNext()) row1 = it1.next()
                if (!row2 && it2.hasNext()) row2 = it2.next()
                int c = row1 && row2 ? comparator.compare(row1, row2) : (row1 ? -1 : 1)
                if (c == 0) {
                    result.write(row1)
                    result.write(row2)
                    row1 = null
                    row2 = null
                } else if (c < 0) {
                    result.write(row1)
                    row1 = null
                } else {
                    result.write(row2)
                    row2 = null
                }
            }
            return result
        } finally {
            page1.close()
            page2.close()
            result.close()
        }
    }


    /**
     * Return a Pipeline where the given column is converted from a string to a java.lang.Double.
     * @param column The name of the column to convert into a Double
     * @return A Pipeline where all rows contains a java.lang.Double at the given column
     */
    Pipeline asDouble(String column) {
        addStep("asDouble(${column})") { row ->
            String value = row[column] as String
            try {
                if (value) row[column] = Double.parseDouble(value)
                return row
            } catch( NumberFormatException ex) {
                return reject( row,"Could not parse ${value} as a Double", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given column into a java.lang.Integer value.
     * @param column containing a string to be turned into a java.lang.Integer
     * @return A Pipeline where all rows contain a java.lang.Integer at given column
     */
    Pipeline asInt(String column) {
        addStep("asInt(${column})") { row ->
            String value = row[column] as String
            try {
                if( value ) row[column] = Integer.parseInt(value)
                return row
            } catch( NumberFormatException ex ) {
                return reject( row,"Could not parse ${value} to an integer.", RejectionCategory.INVALID_FORMAT)
            }
        }
    }

    /**
     * Parses the string value at given column into a java.lang.Boolean value.  It understands values like: Y/N, YES/NO, TRUE/FALSE, 1/0, T/F.
     * @param column containing a string to be turned into a java.lang.Boolean
     * @return A Pipeline where all rows contain a java.lang.Boolean at given column
     */
    Pipeline asBoolean(String column) {
        addStep("asBoolean(${column}") { row ->
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

    /**
     * Parses the string at the given column name into a Date object using the given format.  Any value that
     * cannot be parsed by the format is rejected.  Null values or empty strings are not rejected.
     * @param column The field to use to find the string value to parse
     * @param formats One or more formats of the string to use to parse into a java.util.Date.  The first format
     * that parses without exception will be used. (default format is "yyyy-MM-dd")
     * @return A Pipeline where all rows contain a java.util.Date at given field name
     */
    Pipeline asDate(String column, String... formats = ["yyyy-MM-dd"]) {
        List<SimpleDateFormat> dateFormats = formats.collect { new SimpleDateFormat(it) }
        addStep("asDate(${column}, ${formats})") { row ->
            if(row[column] instanceof Date ) return row
            String val = row[column] as String
            if (val) {
                row[column] = dateFormats.findResult { format ->
                    try {
                        format.parse(val)
                    } catch( ParseException ex ) {
                        return null
                    }
                }
            } else {
                return row // null is a no-op
            }
            return row[column] ? row : reject( row, "${val} could not be parsed by format ${formats}", RejectionCategory.INVALID_FORMAT )
        }
        return this
    }

    /**
     * This writes each row to the specified filename as a CSV separated by the given separator.  It can optionally
     * select a subset of column names from each row.  If unspecified all columns will be saved.
     *
     * @param filename the filename to write the CSV file to
     * @param separator the field separator to use between each field value (default ",")
     * @param columns the list of fields to write from each row.  (default null)
     * @return A Pipeline that returns a row for the csv file.
     */
    public Pipeline save(String filename, String separator = ",", List<String> columns = null ) {
        return this.save( new CsvSink(filename, separator, columns) )
    }

    /**
     * This writes each row to the specified file as a CSV separated by the given separator.  It can optionally
     * select a subset of column names from each row.  If unspecified all columns will be saved.
     *
     * @param csvFile the File to write the CSV file to
     * @param separator the field separator to use between each field value (default ",")
     * @param columns the list of fields to write from each row.  (default null)
     * @return A Pipeline that returns a row for the csv file.
     */
    public Pipeline save(File csvFile, String separator = ",", List<String> columns = null) {
        return this.save( new CsvSink(csvFile, separator, columns) )
    }

    /**
     * Write the rows to a provided {@link gratum.sink.Sink}.  It returns the
     * {@link gratum.etl.Pipeline} that returns the results of the given sink.
     *
     * @param sink the concrete Sink instance to save data to.
     * @return The resulting {@link gratum.etl.Pipeline}.
     */
    public Pipeline save(Sink<Map<String,Object>> sink ) {
        sink.attach( this )

        Pipeline next = new Pipeline( sink.name, this ).source(new ChainedSource(this))
        next.loaded = DO_NOT_TRACK
        after {
            sink.close()
            next.process(sink.result)
            return
        }
        return next
    }

    /**
     * Write out the rows produced to JSON file.
     * @param filename the filename to save the JSON into.
     * @param columns Optional list of columns to use clip out certain columns you want to include in the output.  If left
     * off all columns are included.
     * @return A Pipeline unmodified.
     */
    public Pipeline json(String filename, List<String> columns = null) {
        return save( new JsonSink( new File(filename), columns) )
    }

    /**
     * Write out the rows produced into the given filename as <a href=https://jsonlines.org">JSON Lines</a> format.
     * @param filename the filename to save the JSON lines into.
     * @param columns Optional list of columns to use clip out certain columns you want to include in the output.  If left
     * off all columns are included.
     * @return A Pipeline unmodified.
     */
    public Pipeline jsonl(String filename, List<String> columns = null) {
        return save( new JsonSink( new File(filename), columns).jsonObjectPerLine(true) )
    }

    /**
     * Write out the rows produced into a given File as <a href=https://jsonlines.org">JSON Lines</a> format.
     * @param file the java.io.File to save the JSON lines into.
     * @param columns Optional list of columns to use clip out certain columns you want to include in the output.  If left
     * off all columns are included.
     * @return A Pipeline unmodified.
     */
    public Pipeline jsonl(File file, List<String> columns = null) {
        return save( new JsonSink( file, columns).jsonObjectPerLine(true) )
    }

    /**
     * Prints the values of the given columns for each row to the console, or all columns if no columns are given.
     * @param columns The names of the columns to print to the console
     * @return this Pipeline
     */
    public Pipeline printRow(String... columns) {
        addStep("print()") { row ->
            if( columns ) {
                logger.info( "[ ${columns.toList().collect { row[it] }.join(',')} ]" )
            } else {
                logger.info( "${row.toString()}" )
            }
            return row
        }
        return this
    }

    public Pipeline progress( int col = 50 ) {
        int line = 1
        addStep("progress()") { row ->
            line++
            printf(".")
            if( line % col ) println()
            row
        }
    }

    /**
     * Sets a fieldName in each row to the given value.
     * @param fieldName The new field name to add
     * @param value the value of the new field name
     * @return The Pipeline where each row has a fieldname set to the given value
     */
    public Pipeline setField(String fieldName, Object value ) {
        addStep("setField(${fieldName})") { row ->
            row[fieldName] = value
            return row
        }
        return this
    }
    /**
     * Adds a new field to each row with the value returned by the given closure.
     * @param fieldName The new field name to add
     * @param fieldValue The closure that returns a value to set the given field's name to.
     * @return The Pipeline where the fieldname exists in every row
     */
    public Pipeline addField(String fieldName,
                             @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                             @DelegatesTo(Pipeline) Closure fieldValue) {
        fieldValue.delegate = this
        addStep("addField(${fieldName})") { row ->
            Object value = fieldValue(row)
            if( value instanceof Rejection ) {
                row[REJECTED_KEY] = value
                return row
            }
            row[fieldName] = value
            return row
        }
        return this
    }

    /**
     * Removes a field based on whether the given closure returns true or false.  The closure is optional
     * which will always remove the fieldName if not provided.
     *
     * @param fieldName the name of the field to remove depending on what the optional closure returns
     * @param removeLogic an optional closure that when given can return true or false to indicate to remove
     * the field or not.  If not provided the field is always removed.
     * @return The pipeline where the fieldName has been removed when the removeLogic closure returns true or itself is null.
     */
    public Pipeline removeField(String fieldName,
                                @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                                @DelegatesTo(Pipeline) Closure removeLogic = null) {
        removeLogic?.delegate = this
        addStep( "removeField(${fieldName})") { row ->
            if( removeLogic == null || removeLogic(row) ) {
                row.remove(fieldName)
            }
            return row
        }

        return this
    }

    /**
     * Remove all columns from each row so that only the fields given will be returned.
     * @param columns THe columns names to retain from each row
     * @return The pipeline where only the given columns are returned
     */
    public Pipeline clip(String... columns) {
        addStep( "clip(${columns.join(",")}") { row ->
            row.retainAll { key, value ->
                columns.contains(key)
            }
            return row
        }
        return this
    }

    /**
     * Only allows rows that are unique per the given column.
     *
     * @param column The column name to use for checking uniqueness
     * @return A Pipeline that only contains the unique rows for the given column
     */
    Pipeline unique(String column) {
        Set<Object> unique = [:] as HashSet
        addStep("unique(${column})") { row ->
            if( unique.contains(row[column]) ) {
                return reject(row, "Non-unique row returned", RejectionCategory.IGNORE_ROW)
            }
            unique.add( row[column] )
            return row
        }
        return this
    }

    /**
     * Injects the Collection&lt;Map&gt; returned from the given closure into the downstream steps as individual rows.
     * The given closure is called for every row passed through the preceding step.  Each member of the returned
     * collection will be fed into downstream steps as separate rows.
     * @param name The name of the step
     * @param closure Takes a Map and returns a Collection&lt;Map&gt; that will be fed into the downstream steps
     * @return The Pipeline that will receive all members of the Iterable returned from the closure.
     */
    public Pipeline inject(CharSequence name,
                           @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                           @DelegatesTo(Pipeline) Closure<Iterable<Map<String,Object>>> closure) {
        Pipeline next = new Pipeline(name, this).source(new ChainedSource( this ))
        closure.delegate = this
        addStep(name) { row ->
            Iterable<Map<String,Object>> result = closure.call( row )
            if( result == null ) {
                row = reject( row, "Unknown Reason", RejectionCategory.REJECTION )
                next.doRejections(row, name, -1)
                return row
            } else {
                Iterable<Map<String,Object>> cc = result
                for( Map<String,Object> r : result ) {
                    if( !r[REJECTED_KEY] ) {
                        next.process( r, -1 )
                    }
                }
            }
            return row
        }
        return next
    }

    /**
     * This takes a closure that returns a Pipeline which is used to feed the Pipeline returned by exchange().  The
     * closure will be called for each row emitted from this Pipeline so the closure could create multiple Pipelines,
     * and all data from every Pipeline will be fed into the returned Pipeline.
     *
     * @param closure A closure that takes a Map and returns a pipeline that it's data will be fed on to the
     * returned pipeline.
     *
     * @return A Pipeline whose records consist of the records from all Pipelines returned from the closure
     */
    public Pipeline exchange( @DelegatesTo(Pipeline)
                              @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                              Closure<Pipeline> closure) {
        Pipeline next = new Pipeline( name, this ).source(new ChainedSource(this))
        addStep("exchange(${next.name})") { row ->
            Pipeline pipeline = closure( row )
            pipeline.addStep("Exchange Bridge(${pipeline.name})") { current ->
                next.process( current )
                return current
            }
            pipeline.start()
            return row
        }
        return next
    }

    /**
     * Delegates to {@link #inject(java.lang.String, groovy.lang.Closure)} with a default name.
     * @param closure Takes a Map and returns a Collection&lt;Map&gt; that will be fed into the downstream steps
     * @return The Pipeline that will received all members of the Collection returned from the closure.
     */
    public Pipeline inject( @DelegatesTo(Pipeline)
                            @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>"])
                            Closure closure) {
        this.inject("inject()", closure )
    }

    /**
     * Sets the destination column to the value within the given Map when the destination column is empty/null.
     * @param defaults A Map of destination column to value where destination column is key and the values are the default
     * values used when the destination column is empty/null.
     * @return A Pipeline where the destination columns are initialized to the values in the Map when the destination
     * column is empty.
     */
    public Pipeline defaultValues( Map<String,Object> defaults ) {
        this.addStep("defaultValues for ${defaults.keySet()}") { row ->
            defaults?.each { String column, Object value ->
                if( !row[column] ) row[column] = value
            }
            return row
        }
    }

    /**
     * Sets the destination column to the value of the source column if it is empty or null.
     * @param defaults A Map containing the destination column as the key and source column as the value.
     * @return A pipeline where the rows will have the destination columns set to the source column if empty/null.
     */
    public Pipeline defaultsBy( Map<String,String> defaults ) {
        this.addStep("defaultsBy for ${defaults.keySet()}") { row ->
            defaults?.each { String destColumn, String srcColumn ->
                if( !row[destColumn] ) row[destColumn] = row[srcColumn]
            }
            return row
        }
    }

    /**
     * Limit the number of rows you take from a source to an upper bound.  After the upper bound is hit it will either
     * stop processing rows immediately or continue processing rows but simply reject the remaining based on if halt
     * is true or false, respectively.  If it's rejected rows then all steps above the limit will be executed for
     * all rows.  All steps after the limit operation will only process limit number of rows.
     *
     * @param limit An upper limit on the number of rows this pipeline will process.
     * @param halt after limit has been exceeded stop processing any additional rows (halt = true),
     *              or continue to process rows but simply reject all rows (halt = false).
     * @return A pipeline where only limit number of rows will be sent to down stream steps.
     */
    public Pipeline limit(long limit, boolean halt = true) {
        int current = 0
        this.addStep("Limit(${limit})") { row ->
            current++
            if( current > limit ) {
                if( halt ) {
                    throw new HaltPipelineException("Over the maximum limit of ${limit}")
                } else {
                    return reject(row, "Over the maximum limit of ${limit}", RejectionCategory.IGNORE_ROW)
                }
            }
            return row
        }
    }

    /**
     * Passed a closure that is called with this Pipeline.  This enables you to perform operation
     * on the Pipeline itself without breaking the flow of functional chain.  This does not
     * add a step to the Pipeline.
     * @param The Closure
     * @return this Pipeline
     */
    public Pipeline apply( @ClosureParams( value = FromString, options = ["gratum.etl.Pipeline"])
                            Closure<Pipeline> applyToPipeline) {
        return applyToPipeline.call( this ) ?: this
    }

    /**
     * Replaces all occurrences of the regular expression with given withClause.  The
     * withClause can use $1, $2, $3, etc to refer to groups within the regular expression
     * just like in replaceAll method.
     *
     * @param column the name of the column to pull the value from.
     * @param regEx the regular expression to use for replacing sections of the value.
     * @param withClause the replacement clause swap out the portion matched by the
     * regular expression.
     * @return this Pipeline where the content matched by the regular expression
     * replaced with the given withClause
     */
    public Pipeline replaceAll(String column, Pattern regEx, String withClause) {
        addStep( "replaceAll(${column}, ${regEx.toString()})") { row ->
            String v = row[column]
            row[column] = v?.replaceAll( regEx, withClause )
            return row
        }
    }

    /**
     * Given a column replace all values at that column that match the given
     * key with the value of the values Map.  This basically transform values[key]
     * into values[value] and stores that in the given column.
     *
     * @param column the column to compare against the value map key.
     * @param values the key and value pair that will be substituted given the value at the column
     * @return this Pipeline
     */
    public Pipeline replaceValues(String column, Map<String,String> values ) {
        addStep( "replaceValues(${column}, ${values})" ) { row ->
            String v = row[column]
            if( values.containsKey(v) ) {
                row[column] = values[ v ] ?: row[column]
            }
            return row
        }
    }

    /**
     * Encrypts using PGP a stream on the pipeline and rewrite that stream back to the pipeline.  It looks for
     * a stream on the Pipeline at streamProperty. Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identities, secret key collection,
     * and/or public key collection in order to encrypt.  This will write the encrypted stream back to the Pipeline
     * on the provided streamProperty.  It also adds the file and filename properties to the existing row.
     * @param streamProperty The property that holds a stream object to be encrypted.
     * @param configure The Closure that is passed the PgpContext used to configure how the stream will be encrypted.
     */
    public Pipeline encryptPgp(String streamProperty,
                               @ClosureParams( value = FromString, options = ["gratum.pgp.PgpContext"])
                               @DelegatesTo(Pipeline) Closure configure ) {
        PgpContext pgp = new PgpContext()
        configure.delegate = this
        configure.call( pgp )
        addStep("encrypt(${streamProperty})") { row ->
            File encryptedTemp = File.createTempFile("pgp-encrypted-output-${streamProperty}".toString(), ".gpg")
            InputStream stream = row[streamProperty] as InputStream
            try {
                encryptedTemp.withOutputStream { OutputStream out ->
                    pgp.encrypt((String) row.filename, new Date(), stream, out)
                }
            } finally {
                stream.close()
            }
            if( pgp.isOverwrite() ) {
                File f = row?.file as File
                f?.delete()
            }
            row.file = encryptedTemp
            row.filename = encryptedTemp.getName()
            row[streamProperty] = new FileOpenable(encryptedTemp)
            return row
        }
        return this
    }

    /**
     * Decrypts using PGP a stream on the Pipeline and rewrites the stream back onto the Pipeline.  It looks for
     * a stream at the given streamProperty.  Further configuration is performed by the provided Closure
     * that is passed a {@link gratum.pgp.PpgContext}.  You are required to setup the identity passphrase and the secret
     * key collection used to decrypt. It also adds the file and filename properties to the existing row.
     * @param streamProperty The property within the row on the Pipeline that stores a stream.
     * @param configure The closure called with a PgpContext object to further configure how it will decrypt the stream.
     * @return a Pipeline where the streamProperty contains decrypted stream.
     */
    public Pipeline decryptPgp(String streamProperty,
                               @ClosureParams( value = FromString, options = ["gratum.pgp.PgpContext"])
                               @DelegatesTo(Pipeline) Closure configure ) {
        PgpContext pgp = new PgpContext()
        configure.delegate = this
        configure.call( pgp )
        addStep("decrypt(${streamProperty})") { row ->
            InputStream stream = row[streamProperty] as InputStream
            File decryptedFile = File.createTempFile("pgp-decrypted-output-${streamProperty}", "out")
            try {
                decryptedFile.withOutputStream { OutputStream out ->
                    pgp.decrypt( stream, out )
                }
            } finally {
                stream.close()
            }

            // todo should we get the original file name??!!
            row.file = decryptedFile
            row.filename = decryptedFile.name
            row[streamProperty] = new FileOpenable( decryptedFile )
            return row
        }
    }

    /**
     * Reduces all upstream rows into a value that is passed into the given closure (similar to Groovy inject method).
     * The downstream result is the value returned from the closure's final invocation.  The downstream operators will
     * see only 1 row object.
     * @param name The given name of this step
     * @param value The initial value passed into the given closure.
     * @param logic The closure that is invoked given the current value and the current row.
     * @return the value of the next current value or the final value that will be sent to downstream operators.
     */
    Pipeline reduce(String name, Map<String,Object> value,
                    @DelegatesTo(Pipeline)
                    @ClosureParams( value = FromString, options = ["java.util.Map<String,Object>,java.util.Map<String,Object>"] )
                    Closure<Map<String,Object>> logic) {
        Pipeline downstream = new Pipeline(name, this).source(new ChainedSource(this))
        downstream.loaded = DO_NOT_TRACK
        Map<String,Object> current = value
        addStep("reduce(${name})") { row ->
            current = logic.call( current, row )
            row
        }
        after {
            downstream.process(current, 1)
            return
        }
        return downstream
    }

    /**
     * Collects, or flattens, a set of rows that share the same value in succession within the stream into a List of
     * those matching rows.  This groups rows that share the same value of the given field name.  Then invokes the given
     * windowClosure passing the list of grouped rows.  The windowClosure returns a list of the rows it wants to continue
     * downstream.  This works best with data that has a somewhat predictable order when it comes to the given field name.
     * The flattenWindow operation caches up rows that share the same value for the given field, and once it encounters
     * a row whose given value differs it invokes the windowClosure with the like valued cached rows.  Then it proceeds
     * caching rows that match the different valued row and repeats the same caching logic.
     *
     * @param field the field name to look for common values within.
     * @param windowClosure the logic to use the process the rows that share a successive common value in the field name.
     * @return A downstream pipeline to continue add operations to.
     */
    Pipeline flattenWindow(String field,
                           @DelegatesTo(Pipeline)
                           @ClosureParams( value = FromString.class, options = ["java.util.List<Map<String,Object>>"])
                           Closure<List<Map<String,Object>>> windowClosure ) {
        Map<String,List<Map<String,Object>>> window = [:]
        Pipeline downstream = new Pipeline( name, this ).source( new ChainedSource(this) )
        int line = 0
        windowClosure.delegate = this
        addStep("Window(${field})") { row ->
            if( window[row[field] as String] ) {
                window[row[field] as String].add( row )
            } else {
                if( !window.isEmpty() ) {
                    List<Map<String,Object>> rows = windowClosure( window.entrySet().first().value )
                    rows.each { r -> downstream.process( r, ++line ) }
                    window.clear()
                }
                window[row[field] as String] = [ row ]
            }
            return row
        }
        .after {
            if( !window.isEmpty() ) {
                List<Map<String,Object>> rows = windowClosure( window.entrySet().first().value )
                rows.each { r -> downstream.process( r, ++line ) }
            }
            downstream.finished()
        }
        return downstream
    }

    /**
     * Start processing rows from the source of the pipeline.
     */
    public void start() {
        try {
            src?.start(this)
        } catch( HaltPipelineException ex ) {
            logger.debug("Halting pipeline during steps due to exception.", ex)
        }
    }

    /**
     * Starts processing the rows returned from the {@link gratum.source.Source} into the Pipeline.  When the
     * {@link gratum.source.Source} is finished this method returns the {@link gratum.etl.LoadStatistic} for
     * this Pipeline.
     *
     * @return The LoadStatistic instance from this Pipeline.
     */
    public LoadStatistic go() {
        addDefaultRejections()
        long s = System.currentTimeMillis()
        logger.info("Starting ${name} pipeline")
        start()
        long e = System.currentTimeMillis()
        LoadStatistic stat = toLoadStatistic(s, e)
        logger.info("${stat}")
        return stat
    }

    /**
     * This method is used to send rows to the Pipeline for processing.  Each row passed will start at the first step of
     * the Pipeline and proceed through each step.
     * @param row The row to be processed by the Pipeline's steps.
     * @param lineNumber The lineNumber from the {@link gratum.source.Source} to use when tracking this row through the Pipeline
     */
    public boolean process(Map row, int lineNumber = -1) {
        Map next = row
        for (Step step : processChain) {
            next = step.execute( this, next, lineNumber )
            if( next == null || next[REJECTED_KEY] ) return false
        }
        if( loaded > DO_NOT_TRACK ) loaded++
        return false // don't stop!
    }

    void doRejections(Map<String,Object> current, CharSequence stepName, int lineNumber) {
        if( parent ) {
            parent.doRejections( current, stepName, lineNumber )
        } else {
            Rejection rejection = (Rejection)current[REJECTED_KEY]
            rejection.step = stepName
            rejections?.process(current, lineNumber)
        }
    }

    private String keyOf( Map row, Iterable<String> columns ) {
        return columns.collect { key -> row[key] }.join(":")
    }

    /**
     * Helper method to create a {@link Rejection} object.
     * @param reason A text explanation for what caused the rejection
     * @param category The rejection category to group this specific rejection
     * @return
     */
    public static Rejection reject( String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        return new Rejection( reason, category )
    }

    public static Map reject(Map<String,Object> row, String reason, RejectionCategory category = RejectionCategory.REJECTION ) {
        row[ REJECTED_KEY ] = new Rejection( reason, category )
        return row
    }

    void reject(Map<String,Object> row, int lineNumber = -1) {
        rejections?.process( row, lineNumber )
    }

    LoadStatistic toLoadStatistic(long start, long end) {
        LoadStatistic stat
        if( parent ) {
            stat = parent.toLoadStatistic(start, end)
        } else {
            stat = new LoadStatistic(name: name, start: start, end: end)
        }

        for( Step s : processChain ) {
            s.rejections.each { cat, count ->
                stat.addRejection( cat, s.name, count )
            }

            stat.addTiming(s.name, s.duration )
        }

        if( doneChain ) {
            stat.addTiming("${name}.after", (Long)doneChain.sum() {it.duration } )
        }

        if( loaded > DO_NOT_TRACK ) stat.loaded = loaded
        return stat
    }

    /**
     * Assigned a new source to a Pipeline and returns the pipeline.
     * @param source to use as the Pipeline's source
     * @return this Pipeline.
     */
    Pipeline source(Source source) {
        this.src = source
        return this
    }

    public void finished() {
        try {
            doneChain.each { current ->
                current.execute()
            }
        } catch( HaltPipelineException ex ) {
            logger.debug("Halting pipeline during doneChain due to exception.", ex)
        } finally {
            complete = true
        }
    }

    @CompileDynamic // annoying to do this, but this method trigger a bug in groovy compiler so turned off static compilation for this method
    void addDefaultRejections() {
        if( !parent && !this.rejections ) {
            this.onRejection { rej ->
                rej.addStep("Default SCRIPT_ERROR output") { row ->
                    if( row.rejectionCategory == RejectionCategory.SCRIPT_ERROR ) {
                        logger.error( "${row.rejectionCategory} ${row.rejectionStep} ${row.rejectionReason}\n${row.rejectionException}" )
                    }
                    return row
                }
                return
            }
        } else if( parent ) {
            parent.addDefaultRejections()
        }
    }
}
