# QuestDBClient.jl

Documentation for QuestDBClient.jl

```@meta
CurrentModule = QuestDBClient
```

## Overview

This is a Julia package that can be used to connect to a [QuestDB](https://questdb.io/) database server and send data using the [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/). 


!!! note

    This package is strictly used to write data to the database. Reading is not supported. To read data from QuestDB, you can use QuestDB's [LibPQ](https://github.com/iamed2/LibPQ.jl) or [DBInterface](https://github.com/JuliaDatabases/DBInterface.jl) through port `8812`. Alternatively, you can read the data over through QuestDB's REST API on port `9000`. Visit QuestDB's [docs](https://questdb.io/docs/develop/query-data/) to get more information on how to query data. 


!!! tip

    You can join the QuestDB Community [here](https://questdb.io/community/).


## Installation

You can install the package at the Pkg REPL-mode with:

````julia
(@v1.8) pkg> add QuestDBClient
````

## Quick Examples

### Functional Approach

Using functions to write to a QuestDB Server:

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

## Create ILP records
sender |>
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Omron) |>
    x -> symbol(x, :city => :Nairobi) |>
    x -> FloatColumn(x, :tempareture => 26.8) |> 
    x -> FloatColumn(x, :humidity => 0.51) |>
    x -> AtNow(x)

sender |> 
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Honeywell) |> 
    x -> symbol(x, :city => :London) |>
    x -> FloatColumn(x, :tempareture => 22.9) |> 
    x -> FloatColumn(x, :humidity => 0.254) |>
    x -> AtNow(x)

sender |> 
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Omron) |> 
    x -> symbol(x, :city => :Bristol) |>
    x -> FloatColumn(x, :tempareture => 23.9) |> 
    x -> FloatColumn(x, :humidity => 0.233) |>
    x -> AtNow(x)
    

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

!!! tip

    You can use packages such as [Chain.jl](https://github.com/jkrumbiegel/Chain.jl), [Pipe.jl](https://github.com/oxinabox/Pipe.jl), [Lazy.jl](https://github.com/MikeInnes/Lazy.jl) or any other for function chaining, based on your preference.
    

### Macro based approach

Using macros to write to the QuestDB Server:

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
    @FloatColumn(:tempareture => 24.8) |>
    @FloatColumn(:humidity => 0.334) |>
    @AtNow

sender |>
    @table(:readings) |>
    @symbol(:make => :HoneyWell) |>
    @symbol(:city => :Kisumu) |>
    @FloatColumn(:tempareture => 30.2) |>
    @FloatColumn(:humidity => 0.54) |>
    @AtNow

sender |>
    @table(:readings) |>
    @symbol(:make => :Omron) |>
    @symbol(:city => :Berlin) |>
    @FloatColumn(:tempareture => 26.1) |>
    @FloatColumn(:humidity => 0.45) |>
    @AtNow

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

## Package Manual

```@contents
Pages = [
    "man/functional.md",
    "man/macros.md",
    "man/dataframes.md"
]
```

## API

This client exposes a set of endpoints. However, some need to be prefixed with a module name because of the naming collision with existing `Base` functions.

```@contents
Pages = [
    "lib/sender.md",
    "lib/operators.md",
    "lib/types.md"
]
```