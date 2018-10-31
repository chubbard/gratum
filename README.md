# Gratum ETL
A simplified standalone ETL engine for groovy.  Gratum is groovy + datum.
And gratum just happens to mean thank you or pleasant in Latin.  We built 
gratum with a couple of beliefs about data transformations.

1. Groovy is a great language that easy to get things done quickly.
2. Given a groovy shell and a good ETL DSL we can perform ETL is any environment.
3. Functional programming and pipeline architecture can handle any transformation.

## Installation

For Gradle:

     compile group: 'com.github.chubbard', name: 'gratum', version: '0.5'

For Maven:

      <dependency>
        <groupId>com.github.chubbard</groupId>
        <artifactId>gratum</artifactId>
        <version>0.3</version>
      </dependency>
      
## Oh Shell Yeah!

Gratum is meant to be usable in the groovy shell for getting things done quickly.  
We love awk and sed for quickly manipulating files, but we aren't always working on 
unix like systems.  And writing bash scripts to preserve evertying we're doing is cumbersome.
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

    groovy:000> mport groovy.grape.Grape
    groovy:000> Grape.grab(group: 'com.github.chubbard', module:'gratum')
    groovy:000> import gratum.etl.*
    groovy:000> import static gratum.source.CsvSource.*
    groovy:000> csv('sample.csv').go

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

    ----
    Rejections by category
    IGNORE_ROW: 3
    
    ----
    ==> Collection(6)
    loaded 3
    rejected 3
    took 3 ms
    
#### Filter using a closure

    csv('bank_balance.csv').asDouble('amount').filter { Map row -> row.amount > 500 }.go
    
In the above we load a file named bank_balance.csv then we convert the column `amount` 
to a double value.  Then we filter on `amount` for all rows containing an `amount` 
greater than 500.

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
the Pipeline.  The `CollectionSource.from` method takes in a collection as
the source for the row data.

    from([
            [ name: 'Chuck', gender: 'Male'],
            [ name: 'Jane', gender: 'Female'],
            [ name: 'Rob', gender: 'Male'],
            [ name: 'Charlie', gender: 'Female'],
            [ name: 'Sue', gender: 'Male'],
            [ name: 'Jenny', gender: 'Female']
        ])
       .addStep { Map row ->
           println( row )
           return row
       }
       .go
       
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

A Pipeline doesn't have to process every row.  This could be on purpose
or because the data doesn't match expectations, a script error, etc.  Below
is the example for how to reject a row:

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ])
       .addStep { Map row ->
          return row.gender == 'Female' ? row : null 
       }
       .go
      
The example above shows how a step can reject a row by returning null, but
there is a better way.  

What is often most important is the reason for why a rejections occurred.  In
the examples above we rejected by returning null, but that doesn't tell us
why something was rejected.  Using the `reject` method we can specify more
detail about why a row was rejected.

    from([
            [ name: 'Chuck', gender: 'Male', age: 40],
            [ name: 'Jane', gender: 'Female', age: 25],
            [ name: 'Rob', gender: 'Male', age: 33],
            [ name: 'Charlie', gender: 'Female', age: 55],
            [ name: 'Sue', gender: 'Male', age: 65],
            [ name: 'Jenny', gender: 'Female', age: 43]
        ])
       .addStep { Map row ->
          return row.gender == 'Female' ? row : reject("Rejected gender ${row.gender}", RejectionCategory.IGNORE_ROW) 
       }
       .go

This allows you to provide a rejection category and a free form description.  Using categories is a great
way to quickly group types of rejections into more easily managed sets.

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
        ])
       .addStep('Filter by Gender = Female') { Map row ->
          return row.gender == 'Female' ? row : reject("Rejected gender ${row.gender}", RejectionCategory.IGNORE_ROW) 
       }
       .go

The step name is provided as the first argument to the `addStep` method.  When a rejection is returned from 
a step.  It is added to the Rejection.

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
        ])
        .filter( gender: 'Male' )
        .onRejection { Pipeline rejections ->
            rejections.save( 'rejections.csv', '|')
        }
       .go

In this example of non-Male rows will be rejected by the `filter` method.  Then the `onRejection` registers
a closure, but instead of the normal Map argument it takes another Pipeline.  That pipeline is the rejections
pipeline, and we can add steps and call operations on it just like any normal pipeline.  But what travels over
that Pipeline are all of the rejected rows from the parent Pipeline.  In this example, we used the `save` 
method to write out a pipe-delimited text file (ie a CSV).

What's different about the rows that travel through the rejection pipeline is that they have 3 addition columns:

1. **rejectionCategory**
2. **rejectionReason**
3. **rejectionStep**

The rest of the columns are the original columns from the row object.

### Let's go!

In all of the examples our Pipeline chains have ended with a method `go`.  The `go` method is important
as it starts the processing of the `Source`.  The `go` method executes the processing of the chain, but
it also returns an instance of `LoadStaistic` which gives us statistics about the processing of the 
`Source`.  For example, it contains total number of rows that loaded, rows that were rejected, total
amount of time used while processing the `Source`, step timings, or rejections by categories.

### Operations

It's much easier to use the existing operation methods that are included in the Pipeline.  For example,
we used an `addStep` to add a step that filtered out rows.  This is so common there is already an operation
that does this for you called `filter`.  There are plenty of existing methods to perform common operations.

#### Column Manipulation

addField

removeField

renameField

#### Data Types

asInt

asDouble

asBoolean

#### Filtering

filter

filter



#### 
    