# Macro Approach

The `QuestDBClient` can be used with macros to write data to a QuestDB server instance.

## API

The API that describes the implementation this macro-based approach can be found here:

```@contents
Pages = ["../lib/types.md"]
```

## Example

!!! note 

    The functions need to following order:
        `table -> symbol -> others -> At/AtNow`
    
    `others` represent field types such as integers, floats etc. The **terminal** symbol for each
    chain **should** be `@At()` or `@AtNow()`. 

    Not following the specified order will result in an **exception** being thrown.

### Basic Example 

````julia
using QuestDBClient

"""
Assumes the presence of a table called readings created using:

CREATE TABLE readings (
  timestamp TIMESTAMP,
  city SYMBOL,
  temperature DOUBLE,
  humidity DOUBLE,
  make SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;
"""

## Connects to the localhost at port 9009
sender = Sender()

## Connect the sender to the server first
connect(sender)

## Create ILP record statements
sender |>
    @table(:readings) |>
    @symbol(:make => :Omron) |>
    @symbol(:city => :Lisbon) |>
    @FloatColumn(:temperature => 24.8) |>
    @FloatColumn(:humidity => 0.334) |>
    @AtNow

sender |>
    @table(:readings) |>
    @symbol(:make => :HoneyWell) |>
    @symbol(:city => :Kisumu) |>
    @FloatColumn(:temperature => 30.2) |>
    @FloatColumn(:humidity => 0.54) |>
    @AtNow

sender |>
    @table(:readings) |>
    @symbol(:make => :Omron) |>
    @symbol(:city => :Berlin) |>
    @FloatColumn(:temperature => 26.1) |>
    @FloatColumn(:humidity => 0.45) |>
    @AtNow

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

### Working with DataFrames

DataFrames can also be used as a datasource in the QuestDBClient. However, some preprocessing
is needed such as converting/casting the column types to supported types. The table also needs to be specified
beforehand, the column that represents the designated timestamp and any symbols(tags) need to be specified too.

Supported types include: `Symbol`, `Integer` and subtypes, `AbstractFloat` and subtypes, `Bool`, `Char`, `AbstractString` and subtypes,
        `Date`, `DateTime`, `UUID`.


#### Example

!!! note

    This example requires the installation of the [DataFrames.jl](https://github.com/JuliaData/DataFrames.jl) package.

A DataFrame object with the following structure will be used in the example:

| city       | make       | tempareture  |   humidity  |
|:-----------|:-----------|-------------:|------------:|
| London     | Omron      |  29.4        |    0.334    |
| Nairobi    | Honeywell  |  24.0        |    0.51     |



````julia
using DataFrames
using QuestDBClient

## Create a DataFrame instance
df = DataFrame(city=["London", "Nairobi"], 
               make=[:Omron, :Honeywell], 
               temperature=[29.4, 24.0], 
               humidity=[0.334, 0.51])

## Create a sender instance that will connect to the localhost at port 9009
sender = Sender()

## Connect the sender to the server first
connect(sender)

## Map the dataframe data to ILP record statements
sender |> @source(df = df, table = :readings, symbols=[:city, :make])

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

An example with the `At` field specified.

| city       | make       | tempareture  |   humidity  | collection_time          |
|:-----------|:-----------|-------------:|------------:|-------------------------:
| London     | Omron      |  29.4        |    0.334    | 2023-04-10T13:09:31Z     |
| Nairobi    | Honeywell  |  24.0        |    0.51     | 2023-04-10T13:09:42Z     |


````julia
using DataFrames
using Dates
using QuestDBClient

## A DataFrame instance
df = DataFrame(city=["London", "Nairobi"], 
               make=[:Omron, :Honeywell], 
               temperature=[29.4, 24.0], 
               humidity=[0.334, 0.51], 
               collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])

## Cast the collection_time to DateTime
date_format = dateformat"y-m-dTH:M:SZ"
df[!, :collection_time] = DateTime.(df[:, :collection_time], date_format)

## Create a sender instance that will connect to the localhost at port 9009
sender = Sender()

## Connect the sender to the server first
connect(sender)

## Map the dataframe data to ILP record statements
sender |> @source(df = df, table = :readings, symbols = [:city, :make], at = :collection_time)

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

!!! note

    The `sender` attempts to write values to the `QuestDB Database Server` depending
    on whether the buffer size has been met or exceeded while reading the rows of the 
    `DataFrame`. This is even before the `flush` or `close` function is called.