# QuestDBClient

This is a Julia package tha can be used to connect to a [QuestDB](https://questdb.io/) database server and send data using the [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v1.8/write_protocols/line_protocol_tutorial/). 

>This package is strictly used to write data to the database. Reading is not supported. To read data from QuestDB, you can use QuestDB's [LibPQ](https://github.com/iamed2/LibPQ.jl) or [DBInterface](https://github.com/JuliaDatabases/DBInterface.jl) through port `8812`. Alternatively, you can read the data over through QuestDB's REST API on port `9000`. Visit QuestDB's [docs](https://questdb.io/docs/develop/query-data/) to get more information on how to query data. 


**Installation** at the Julia REPL, `using Pkg; Pkg.add("QuestDBClient")`

**Documentation** can be found [here](https://ochibobo.github.io/QuestDBClient.jl/dev/).