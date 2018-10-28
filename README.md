# Gratum ETL
A simplified standalone ETL engine for groovy.  Gratum is groovy + datum.
And gratum just happens to mean thank you or pleasant in Latin.  We built 
gratum with a couple of beliefs about data transformations.

1. Groovy is a great language that easy to get things done quickly.
2. Given a groovy shell and a good ETL DSL we can perform ETL is any environment.
3. Functional programming and pipeline architecture can handle any transformation.

## Installation

For Gradle:

     compile group: 'com.github.chubbard', name: 'gratum', version: '0.3'

For Maven:

      <dependency>
        <groupId>com.github.chubbard</groupId>
        <artifactId>gratum</artifactId>
        <version>0.3</version>
      </dependency>
      
## Oh Shell Yeah!

Gratum is meant to be usable in the groovy shell for getting thigns done quickly.  
We love awk and sed for quickly manipulating files, but we aren't always working on 
unix like systems.  The groovy shell provides us a portable environment to do similar
manipulations.  Here is how to get gratum started up in your shell:

    groovy:000> :grab 'com.github.chubbard:gratum'
    groovy:000> import gratum.etl.*
    groovy:000> import static gratum.source.CsvSource.*
    groovy:000> csv('sample.csv').go

But, to make it easier to get started you'll want to add the following to your 
`$HOME/.groovy/groovysh.rc` file:

    groovy.grape.Grape.grab(group: 'com.github.chubbard', module: 'gratum')
    import gratum.etl.*
    import gratum.source.*
    import static gratum.source.CsvSource.*

## Examples

#### Filter by a column on a single value.

    csv('example1.csv').filter([gender: 'Female']).go
    
#### Filter using a closure

    csv('bank_balance.csv').asDouble('amount').filter { Map row -> row.amount > 500 }.go
    
In the above we load a file named bank_balance.csv then we convert the column `amount` 
to a double value.  Then we filter on `amount` for all rows containing an `amount` 
greater than 500.

#### Sort by column

    csv('bank_balance.csv')
       .asDouble('amount')
       .filter { Map row -> row.amount > 500 }
       .sort('amount')
       .go        

## Digging Deeper

### Baby Steps

So how do Pipelines work?  A pipeline is a series of ordered steps.  Every
Pipeline has a Source.  A source feeds a Pipeline.  In the example below
the `csv` method creates the source and attaches it to a Pipeline.

    csv('example1.csv')
       .addStep { Map row ->
           println( row )
           return row
       }
       .go
       
To add a step to the pipeline use the `addStep` method.  The `addStep` method
takes a closure where the only parameter is a Map object.  That represents a 
single row or line in a CSV file.  Every closure added with the step must 
return a value.  The provided the return value is non-null it will be used 
as the parameter to the next step on the Pipeline.  This let's any step act as
a map like function used in most function programming.  

If a step returns a null then it rejects this row, and no more steps are 
processed afterwards.  The Pipeline goes to the next row and starts over
from the first step.

### Rejections

A Pipeline doesn't have to process every row.  This could be on purpose
or because the data doesn't match expectations, a script error, etc.  Below
is the example for how to reject a row:

    csv('data.csv')
       .addStep { Map row ->
          return row.jobCategory == 'manager' ? row : null 
       }
       .go
      
The example above shows how a step can reject a row by returning null, but
there is a better way.  

What is often most important is the reason for why a rejections occurred.  In
the examples above we rejected by returning null, but that doesn't tell us
why something was rejected.  If we wanted to include more information we
use the reject() method.

    csv('data.csv')
       .addStep { Map row ->
          return row.jobCategory == 'manager' ? row : reject('Row was a non-manager', RejectionCategory.IGNORE_ROW) 
       }
       .go

This allows you to provide a rejection category and a free form description.

### What's in a name?

Knowing why something was rejected is good, but where it was rejected is 
important for debugging why a row is rejected.  All steps have a name to 
identify where in the pipeline a rejection occurs.

    csv('data.csv')
       .addStep('Filter by manager') { Map row ->
           return row.jobCategory == 'manager' ? row : reject('Row was a non-manager', RejectionCategory.IGNORE_ROW)
       }
       .go

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

    