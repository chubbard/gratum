# Gratum ETL
A simplified standalone ETL engine for groovy.  Gratum is groovy + datum.
And gratum just happens to mean thank you or pleasant in Latin.  We built 
gratum with a couple of beliefs about data transformations.

1. Groovy is a great language that easy to get things done quickly.
2. Given a groovy shell and a good ETL DSL we can perform ETL is any environment.
3. Functional programming and pipeline architecture can handle any transformation.

## Installation

For Gradle:

     compile group: 'gratum', name: 'gratum', version: '0.1'

For Maven:

      <dependency>
        <groupId>gratum</groupId>
        <artifactId>gratum</artifactId>
        <version>0.1</version>
      </dependency>
      
## Oh Shell Yeah!

Gratum is meant to be usable in the groovy shell for getting thigns done quickly.  
We love awk and sed for quickly manipulating files, but we aren't always working on 
unix like systems.  The groovy shell provides us a portable environment to do similar
manipulations.  Here is how to get gratum started up in your shell:

    groovy:000> :grab 'gratum:gratum'
    groovy:000> import gratum.etl.*
    groovy:000> import static gratum.source.CsvSource.*
    groovy:000> csv('sample.csv').printStatistics.go

But, to make it easier to get started you'll want to add the following to your 
`$HOME/.groovy/groovysh.rc` file:

    TBD

## Examples



## Digging Deeper