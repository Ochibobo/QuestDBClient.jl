# QuestDBClient

This is a Julia package that can be used to connect to a [QuestDB](https://questdb.io/) database server and send data using the [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/). 

>This package is strictly used to write data to the database. Reading is not supported. To read data from QuestDB, you can use QuestDB's [LibPQ](https://github.com/iamed2/LibPQ.jl) or [DBInterface](https://github.com/JuliaDatabases/DBInterface.jl) through port `8812`. Alternatively, you can read the data over through QuestDB's REST API on port `9000`. Visit QuestDB's [docs](https://questdb.io/docs/develop/query-data/) to get more information on how to query data. 


**Installation** at the Julia REPL, `using Pkg; Pkg.add("QuestDBClient")`

**Documentation** can be found [here](https://ochibobo.github.io/QuestDBClient.jl/dev/).

---
## Basic Examples

### Functional Approach:


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

## Create a sender instance that will connect to the localhost at port 9009
sender = Sender()

## Connect the sender to the server first
connect(sender)

## Create ILP records
sender |>
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Omron) |>
    x -> symbol(x, :city => :Nairobi) |>
    x -> FloatColumn(x, :temperature => 26.8) |> 
    x -> FloatColumn(x, :humidity => 0.51) |>
    x -> AtNow(x)

sender |> 
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Honeywell) |> 
    x -> symbol(x, :city => :London) |>
    x -> FloatColumn(x, :temperature => 22.9) |> 
    x -> FloatColumn(x, :humidity => 0.254) |>
    x -> AtNow(x)

sender |> 
    x -> table(x, :readings) |> 
    x -> symbol(x, :make => :Omron) |> 
    x -> symbol(x, :city => :Bristol) |>
    x -> FloatColumn(x, :temperature => 23.9) |> 
    x -> FloatColumn(x, :humidity => 0.233) |>
    x -> AtNow(x)
    

## Flush the output to the server
QuestDBSender.flush(sender)

## Close the socket connection
## Close first calls QuestDBSender.flush(sender) as part of its definition
QuestDBSender.close(sender)
````

### Macro based approach:


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
---
## Roadmap Features
- [ ] Sender Auth and TLS implementation.
  - [ ] Find or implement `ecdsa` in Julia.
- [ ] Error propagation from QuestDB Server.
- [ ] Re-implement the sender's buffer  (it's currently just a `String`).
- [ ] Extend `DataFrames` support to support of Julia's `Table Interface`.
- [ ] Allow for the extending of the API by the user who can add types and define how they'll be added to an ILP record statement. 
