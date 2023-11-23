

# Gratum ETL

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.chubbard/gratum/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.github.chubbard/gratum)
[![Build Status](https://travis-ci.com/chubbard/gratum.svg?branch=master)](https://travis-ci.com/chubbard/gratum)

A simplified standalone ETL engine for groovy.  Gratum is groovy + datum.
And gratum just happens to mean thank you or pleasant in Latin.  We built 
gratum with a couple of beliefs about data transformations.

1. Groovy is a great language that easy to get things done quickly.
2. Given a groovy shell and a good ETL DSL we can perform ETL is any environment.
3. Functional programming and pipeline architecture can handle any transformation.

## Installation

For Gradle:

     compile group: 'com.github.chubbard', name: 'gratum', version: '1.1.5'

For Maven:

      <dependency>
        <groupId>com.github.chubbard</groupId>
        <artifactId>gratum</artifactId>
        <version>1.1.5</version>
      </dependency>
      
## Oh Shell Yeah!

Gratum is meant to be usable in the groovy shell for getting things done quickly.  
We love awk and sed for quickly manipulating files, but we aren't always working on 
unix like systems.  And writing bash scripts to preserve everything we're doing is cumbersome.
The groovy shell provides us a portable environment to do similar manipulations.

Make sure you have your `~/.groovy/grapeConfig.xml` setup first.  If you need a little refresher
here you go:

    <ivysettings>
        <settings defaultResolver="downloadGrapes"/>
        <resolvers>
            <chain name="downloadGrapes">
                <ibiblio name="local" root="file:${user.home}/.m2/repository/" m2compatible="true"/>    	
                <ibiblio name="ibiblio" m2compatible="true"/>
                <ibiblio name="java.net2" root="http://download.java.net/maven/2/" m2compatible="true"/>
                <!-- uncomment if you want SNAPSHOTS
                <ibiblio name="maven.snapshot" root="https://oss.sonatype.org/content/repositories/snapshots/" m2compatible="true"/>
                -->
            </chain>
        </resolvers>
    </ivysettings> 
  
Here is how to get gratum started up in your shell:

    groovy:000> import groovy.grape.Grape
    groovy:000> Grape.grab(group: 'com.github.chubbard', module:'gratum')
    groovy:000> import gratum.etl.*
    groovy:000> import static gratum.source.CsvSource.*
    groovy:000> csv('sample.csv').into().go

But, to make it easier to get started you'll want to add the following to your 
`$HOME/.groovy/groovysh.rc` file:

    groovy.grape.Grape.grab(group: 'com.github.chubbard', module: 'gratum')
    import gratum.etl.*
    import gratum.source.*
    import static gratum.source.CsvSource.*
    import static gratum.source.CollectionSource.*
    import static gratum.source.HttpSource.*
    import static gratum.source.ZipSource.*

## Examples

#### Filter by a column on a single value.

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
        filter([gender: 'Female']).
        go()

That should produce the following:

```
----
Rejections by category
IGNORE_ROW: 3
    filter gender -> Female: 3

----
==> Collection(6)
loaded 3
rejected 3
took 0s 4ms
```

#### Filtering by multiple columns

    from([
            [ name: 'Chuck', gender: 'Male', city: 'London'],
            [ name: 'Jane', gender: 'Female', city: 'London'],
            [ name: 'Rob', gender: 'Male', city: 'Manchester'],
            [ name: 'Charlie', gender: 'Female', city: 'Liverpool'],
            [ name: 'Sue', gender: 'Male', city: 'Oxford'],
            [ name: 'Jenny', gender: 'Female', city: 'Oxford']
        ]).
        filter([gender: 'Female', city: 'Oxford']).
        go()

That should produce the following:

```
----
Rejections by category
IGNORE_ROW: 3
    filter gender -> Female,city -> Oxford: 5

----
==> Collection(6)
loaded 1
rejected 5
took 0s 1ms
```

#### Filtering using a Collection

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
        filter([name: ['Chuck', 'Jane', 'Rob']]).
        go()

That should produce the following:

```
----
Rejections by category
IGNORE_ROW: 3
	filter name -> [Chuck, Jane, Rob]: 3

----
==> Collection(6) 
loaded 3 
rejected 3 
took 0s 0ms
```
#### Filter using a Regular Expression

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
        filter([name: ~/Ch.*/]).
        go()

That should produce the following:

```
----
Rejections by category
IGNORE_ROW: 4
	filter name -> Ch.*: 4

----
==> Collection(6) 
loaded 2 
rejected 4 
took 0s 1ms
```    
#### Filter using a closure

    csv('bank_balance.csv').into().asDouble('amount').filter { Map row -> row.amount > 500 }.go

In the above we load a file named bank_balance.csv then we convert the column `amount`
to a double value.  Then we filter on `amount` for all rows containing an `amount`
greater than 500.

#### Filter using closures and maps

You can mix closures with the map version of the `filter` method.  Here is an example:

    from([
            [ name: 'Chuck', gender: 'Male', age: 33],
            [ name: 'Jane', gender: 'Female', age: 24],
            [ name: 'Rob', gender: 'Male', age: 28],
            [ name: 'Charlie', gender: 'Female', age: 35],
            [ name: 'Sue', gender: 'Male', age: 50],
            [ name: 'Jenny', gender: 'Female', age: 42]
        ]).
        filter([gender: 'Male', age: { v -> v > 30 }]).
        go()

That should produce the following:

```
----
Rejections by category
IGNORE_ROW: 4
	filter gender -> Male,age -> {}: 4

----
==> Collection(6) 
loaded 2 
rejected 4 
took 0s 0ms
```

#### Sort by column

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
       filter { Map row -> row.age > 50 }.
       sort('age').
       printRow().
       go()        

## Digging Deeper

### Baby Steps

So how do Pipelines work?  A pipeline is a series of ordered steps.  Every
Pipeline has a Source.  A source feeds a Pipeline.  In the example below
the `from` method creates the source, attaches it to a Pipeline, and returns
the Pipeline.  The [`CollectionSource.from`](https://chubbard.github.io/gratum/groovydoc/gratum/source/CollectionSource.html)
method takes in a collection as the source for the row data.

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
       addStep { Map row ->
           println( row )
           return row
       }.
       go()
       
To add a step to the pipeline use the `addStep` method.  The `addStep` method
takes a closure where the only parameter is a Map object.  That represents a 
single row or line we're processing.  Every closure added with the step must 
return a value.  Provided the return value is non-null it will be used 
as the parameter to the next step on the Pipeline.  This let's any step act as
a transformation function.  

If a step returns a null then it rejects this row, and no more steps are 
processed afterwards.  The Pipeline goes to the next row and starts over
from the first step.

### Rejections

A Pipeline does not have to process every row.  This could be on purpose
or because the data does not match expectations, a script error, etc.  Below
is the example for how to reject a row:

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
       addStep { Map row ->
          return row.gender == 'Female' ? row : null 
       }.
       go()
      
The example above shows how a step can reject a row by returning null, but
there is a better way.  

What is often most important is the reason for why a rejections occurred.  In
the examples above we rejected by returning null, but that doesn't tell us
why something was rejected.  Using the [`reject`](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#reject(java.util.Map,java.lang.String,%20gratum.etl.RejectionCategory))
method we can specify more detail about why a row was rejected.

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
       addStep { Map row ->
          return row.gender == 'Female' ? row : reject(row, "Rejected gender ${row.gender}", RejectionCategory.IGNORE_ROW) 
       }.
       go()

This allows you to provide a [`RejectionCategory`](https://chubbard.github.io/gratum/groovydoc/gratum/etl/RejectionCategory.html) 
and a free form description.  Using categories is a great way to quickly group types of 
rejections into more easily managed sets.

### What's in a name?

Knowing why something was rejected is good, but where it was rejected is important for 
pinpointing which step produced the rejection.  All steps have a name to 
identify where in the pipeline a rejection occurs.

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
       addStep('Filter by Gender = Female') { Map row ->
          return row.gender == 'Female' ? row : reject(row, "Rejected gender ${row.gender}", RejectionCategory.IGNORE_ROW) 
       }.
       go()

The step name is provided as the first argument to the `addStep` method.  When a rejection is returned from 
a step.  The step's name is added to the Rejection.

### Processing Rejections

Depending on your task you may or may not care about rejections, but eventually you may run into a situation
where you need to understand why something was rejected.  Hooking into the rejection pipeline is possible
with the `onRejection` method.  Here is an example:

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
        filter( gender: 'Male' ).
        onRejection { Pipeline rejections ->
            rejections.save( 'rejections.csv', '|')
        }.
        go()

In this example of non-Male rows will be rejected by the `filter` method.  Then the `onRejection` registers
a closure, but instead of the normal Map argument it takes another Pipeline.  That pipeline is the rejections
pipeline, and we can add steps and call operations on it just like any normal pipeline.  But what travels over
that Pipeline are all the rejected rows from the parent Pipeline.  In this example, we used the `save` 
method to write out a pipe-delimited text file (ie a CSV).

What's different about the rows that travel through the rejection pipeline is that they have 3 addition columns:

1. **rejectionCategory**
2. **rejectionReason**
3. **rejectionStep**

The rest of the columns are the original columns from the row object.

### Let's go Already!

In all the examples our Pipeline chains have ended with a method `go`.  The `go` method is important
as it starts the processing of the `Source`.  The `go` method executes the processing of the chain, but
it also returns an instance of `LoadStaistic` which gives us statistics about the processing of the 
`Source`.  For example, it contains total number of rows that loaded, rows that were rejected, total
amount of time used while processing the `Source`, step timings, or rejections by categories.

    LoadStatistic stats = from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ]).
        filter( gender: 'Male' ).
        go()
    println( stats ) 

This yields the following:

```
----
Rejections by category
IGNORE_ROW: 3
	filter gender -> Male: 3

----
==> Collection(6) 
loaded 3 
rejected 3 
took 0s 0ms
```

The upper section is the rejections by category.  As discussed above rejections have categories, so we group
all rejections by their common categories and list the counts there.  The next section shows total rows
that loaded, the total rejections, and the total time it took to process the source.  Above those stats is the 
name of the pipeline.

## Pipeline troubleshooting

So sometimes it helps to know where rejections occurred or performance issues might arise in your Pipeline.  The default

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
        filter([gender: 'Female']).
        go()

That should produce the following which is the default information provided:

```
----
Rejections by category
IGNORE_ROW: 3
    filter gender -> Female: 3

----
==> Collection(6)
loaded 3
rejected 3
took 0s 4ms
```
But, if we wanted a breakdown of all steps involved and timing information we can do the following to include more details
when printing out the `LoadStatistic` object.
```
    LoadStatistic stat = from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ]).
        filter([gender: 'Female']).
        go()
    println( stat.toString(true) )
```
Will produce a full break down of the steps on the Pipeline.
```
----
Step Stats
| Step Name                      | Loaded     | Rejected   | Avg Duration (ms) | Total Duration (ms)  |
-------------------------------------------------------------------------------------------------------
| filter gender -> Female        |          3 |          3 |              0.17 |                    1 |

----
Rejections by category
IGNORE_ROW: 3
	filter gender -> Female: 3

----
==> Collection(6) 
loaded 3 
rejected 3 
took 0s 1ms
```

In this example, we can see that the only step on this Pipeline is the filter step we added.  And that step filtered out
3 rows, and passed (ie loaded) 3 rows.  We also see the average duration in milliseconds and the total duration in 
milliseconds.  The average duration is the total duration divided by the total number of rows that passed through this 
step.  This gives you an average time each pass through the step.  It makes it easier to compare each step for performance
cost since not all steps will see the same number of rows given that rejections could reduce the downstream rows being
processed.

## Concurrency

Pipelines are single threaded using the calling thread of `go()` to execute the Pipeline.  Version 1.2.x
introduced `LocalConcurrencContext` which allows you to spread out a Pipeline's elements across a pool of
workers to process data.  Here is a quick example of how to do that:

```
int workers = 4
int queueSize = 50
LocalConcurrentContext context = new LocalConcurrentContext(workers, queueSize)
LoadStatistic stats = csv("titantic.csv", ",")
        .apply( 
            context.spread { pipeline ->
                pipeline.addStep("Assert we are on another thread") { Map row ->
                    assert Thread.currentThread().name.startsWith("Worker")
                    return row
                }
            }
            .collect { pipeline ->
                pipeline.addStep("Assert we are on the results thread") { Map row ->
                    assert Thread.currentThread().name.startsWith("Results")
                    return row
                }
            }
            .connect()
        )
        .go()
println( stats )
```
So this instantiates a `LocalConcurrentContext` which will allow us to define the Pipeline for workers
and the Pipeline that will collect the results from those workers.  The `LocalConcurrentContext` constructor
takes the number of workers you want to create and the size of the queue to use.  The defaults are 8 workers and 200
size queue.  So in the above code you can see we're overriding that to define just 4 workers and a queue of size 50.
The queue is a blocking queue so once the queue fills up the producer thread (ie the calling thread of `go()`) will block
until the workers drain the queue, and it will resume feeding elements into the queue.  In this case the producer logic
is above the call to `apply` which in this case it's just reading a simple `csv`, but it could be more complicated.

The call to `apply` allows us to pass a Closure that configures this Pipeline.  `apply()` can be used as a method for
building reusable section of steps to be placed on the `Pipeline`.  In this case `LocalConcurrentContext` is doing just that
except we're going to configure it before we do so.

`LocalConcurrentContext` provides to configure methods `spread` and `collect`.  These methods are how you set up the worker's
Pipeline and the collection `Pipeline` which recollects the results from the workers.  This is very similar to fork join or 
MapReduce where `spread` is used to define the worker's Pipeline (ie Map), and `collect` is to define the result's processor
(ie Reduce).

In the example above each method adds a single step to their respective `Pipline`s that added just a simple assert method
to verify they are being called on the thread they expect.  So `spread` looks to see that it's called on a thread with
`"Worker"` in the name.  And `collect` is being called with a thread with `"Results"` in the name.  Note that the `collect`
`Pipeline` is using a different thread than the calling thread!

### Spread

The `spread` method is used to define the Pipeline used for the worker threads.  The closure passed to the spread method 
will be called for each worker thread.  Therefore, each thread will have it's down Pipeline instance.  It's safe to
utilize local variables within the spread closure, but it's not safe to use variables from outside the closure method
without using threading protections (synchronization, semaphores, thread locals, etc).

### Collect

The `collect` method is used to define the Pipeline used for the Results thread.  There is only 1 result thread.  All
successful results from the worker threads are passed through the result Pipeline.  If a worker thread rejects any row
those are passed through the rejection Pipeline on the Results thread.

### Connect

The `connect` method attaches the `LocalConcurrentContext` to the existing Pipeline. This method is
**required** to be called so that rows from the Pipeline are routed through the `LocalConcurrentContext` for processing.
Without this call nothing will be sent to the worker threads and result thread.

## Operations

It's much easier to use the existing operation methods that are included in the Pipeline.  For example,
we used an `addStep` to add a step that filtered out rows.  This is so common there is already an operation
that does this for you called `filter`.  There are plenty of existing methods to perform common operations.

## API Docs

[groovydoc](https://chubbard.github.io/gratum/groovydoc)

[javadoc](https://chubbard.github.io/gratum/javadoc)

### Basics

[addStep](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#addStep(java.lang.String,%20Closure%3CMap%3E))

#### Column Manipulation

[addField](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#addField(java.lang.String,%20groovy.lang.Closure))

[renameFields](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#renameFields(java.util.Map))

[setField](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#setField(java.lang.String,%20java.lang.Object))

[clip](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#clip(java.lang.String))

[defaultValues](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#defaultValues(java.util.Map))

[defaultsBy](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#defaultsBy(java.util.Map))

#### Data Types

[asInt](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#asInt(java.lang.String))

[asDouble](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#asDouble(java.lang.String))

[asBoolean](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#asBoolean(java.lang.String))

[asDate](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#asDate(java.lang.String,%20java.lang.String))

#### Filtering

[filter(Map)](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#filter(java.util.Map))

[filter(Closure)](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#filter(groovy.lang.Closure))

[groupBy](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#groupBy(java.lang.String))

### Data Manipulation

[sort](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#sort(java.lang.String))

[trim](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#trim())

[unique](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#unique(java.lang.String))

[limit](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#limit(long,boolean))

[replaceAll](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#replaceAll(java.lang.String,%20java.util.regex.Pattern,%20java.lang.String))

[replaceValues](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#replaceValues(java.lang.String,%20Map%3CString,%20String%3E))

[window](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#window(java.lang.String,%20groovy.lang.Closure%3CList%3E))

### Branching

[branch](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#branch(Closure%3CVoid%3E))

[branch](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#branch(Map%3CString,%20Object%3E,%20Closure%3CVoid%3E))

[onRejection](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#onRejection(Closure%3CVoid%3E))

#### Pipeline Manipulation

[concat](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#concat(gratum.etl.Pipeline))

[exchange](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#exchange(Closure%3CPipeline%3E))

[inject(Closure)](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#inject(groovy.lang.Closure))

[inject(String, Closure)](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#inject(java.lang.String,%20groovy.lang.Closure))

[intersect](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#intersect(gratum.etl.Pipeline,%20def))

[join](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#join(gratum.etl.Pipeline,%20def,%20boolean))

[fillDownBy](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#fillDownBy(Closure%3CBoolean%3E))

[configure](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#configure(Closure%3CPipeline%3E))

[apply](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#apply(Closure%3CVoid%3E))

### Concurrency

[LocalConcurrentContext](https://chubbard.github.io/gratum/groovydoc/gratum/concurrency/LocalConcurrentContext.html)

### Output

[save](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#save(java.lang.String,%20java.lang.String,%20List%3CString%3E))

[save](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#save(Sink%3CMap%3CString,%20Object%3E%3E))

[printRow](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#printRow(java.lang.String))

[encryptPgp](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#encryptPgp(java.lang.String,%20groovy.lang.Closure))

[decryptPgp](https://chubbard.github.io/gratum/groovydoc/gratum/etl/Pipeline.html#decryptPgp(java.lang.String,%20groovy.lang.Closure))

## Sources

Operations are great, but you need data for those operations to work on.  Sources are how that data is 
passed into the Pipeline.  These are the Sources you can use to provide data.

[csv](https://chubbard.github.io/gratum/groovydoc/gratum/source/CsvSource.html)

[json](https://chubbard.github.io/gratum/groovydoc/gratum/source/JsonSource.html)

[file](https://chubbard.github.io/gratum/groovydoc/gratum/source/FileSystemSource.html)

[xls](https://chubbard.github.io/gratum/groovydoc/gratum/source/XlsSource.html)

[xlsx](https://chubbard.github.io/gratum/groovydoc/gratum/source/XlsxSource.html)

[collection](https://chubbard.github.io/gratum/groovydoc/gratum/source/CollectionSource.html)

[zip](https://chubbard.github.io/gratum/groovydoc/gratum/source/ZipSource.html)

[http/https](https://chubbard.github.io/gratum/groovydoc/gratum/source/HttpSource.html)

[jdbc](https://chubbard.github.io/gratum/groovydoc/gratum/source/JdbcSource.html)

[concat](https://chubbard.github.io/gratum/groovydoc/gratum/source/ConcatSource.html)

[ssh](https://chubbard.github.io/gratum/groovydoc/gratum/source/SshSource.html)

[archived](https://chubbard.github.io/gratum/groovydoc/gratum/source/ArchivedSource.html)