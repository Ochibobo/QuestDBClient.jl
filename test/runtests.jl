using QuestDBClient
using Dates
using DataFrames
using Test

@testset "QuestDBSender initialization" begin
    ## Tests around the sender
    sender = Sender()
    ## Equality test
    @test sender == Sender()
    ## Test the address matches
    @test sender.host == "127.0.0.1"
    ## Test the port
    @test sender.port == 9009
    ## Test the default batch size
    @test sender.batchSize == QuestDBClient.QuestDBSender.DEFAULT_BUFFER_SIZE
    ## Test that the tables & fields are empty now
    @test sender.hasTable == false
    @test sender.hasFields == false
    ## Test that the buffer is empty
    @test isempty(sender.buffer)
    ## Test that there is no socket connection
    @test isnothing(sender.socket)
end

@testset "QuestDBOperators basic buffer write" begin
    sender = Sender()

    sender |> 
        x -> table(x, :readings) |> 
        x -> symbol(x, :make => :Omron) |>
        x -> symbol(x, :city => :Nairobi) |>
        x -> FloatColumn(x, :temperature => 26.8) |> 
        x -> FloatColumn(x, :humidity => 0.51) |>
        x -> At(x, DateTime(2023, 4, 1))
    
    records = "readings,make=Omron,city=Nairobi temperature=26.8,humidity=0.51 1680307200000000000\n"

    @test sender.buffer == records 
    
    sender |> 
        x -> table(x, :readings) |> 
        x -> symbol(x, :make => :Honeywell) |> 
        x -> symbol(x, :city => :London) |>
        x -> FloatColumn(x, :temperature => 22.9) |>
        x -> FloatColumn(x, :humidity => 0.254) |>
        x -> At(x, DateTime(2023, 4, 2))

    records *= "readings,make=Honeywell,city=London temperature=22.9,humidity=0.254 1680393600000000000\n"

    @test sender.buffer == records
end

@testset "QuestDBTypes basic buffer writes" begin
    sender = Sender()

    sender |>
        @table(:readings) |>
        @symbol(:make => :Omron) |>
        @symbol(:city => :Nairobi) |>
        @FloatColumn(:temperature => 26.8) |>
        @FloatColumn(:humidity => 0.334) |>
        @At(DateTime(2023, 4, 1))
    
    records = "readings,make=Omron,city=Nairobi temperature=26.8,humidity=0.334 1680307200000000000\n"

    @test sender.buffer == records

    sender |>
        @table(:readings) |>
        @symbol(:make => :Honeywell) |>
        @symbol(:city => :Kisumu) |>
        @FloatColumn(:temperature => 30.2) |>
        @FloatColumn(:humidity => 0.54) |>
        @At(DateTime(2023, 4, 2))
    
    records *= "readings,make=Honeywell,city=Kisumu temperature=30.2,humidity=0.54 1680393600000000000\n"

    @test sender.buffer == records
end

@testset "QuestDBOperators dataframes buffer write" begin
    sender = Sender()

    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0], 
                humidity=[0.334, 0.51],
                collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])

    ## Cast the collection_time to DateTime
    date_format = dateformat"y-m-dTH:M:SZ"
    df[!, :collection_time] = DateTime.(df[:, :collection_time], date_format)
    
    sender |> x -> Source(x, df, :readings, symbols = [:city, :make], at = :collection_time)

    records = "readings,city=London,make=Omron humidity=0.334,temperature=29.4 1681132171000000000\n"
    records *= "readings,city=Nairobi,make=Honeywell humidity=0.51,temperature=24.0 1681132182000000000\n"

    @test sender.buffer == records
end


@testset "QuestDBTypes dataframes buffer write" begin
    sender = Sender()

    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0], 
                humidity=[0.334, 0.51],
                collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])

    ## Cast the collection_time to DateTime
    date_format = dateformat"y-m-dTH:M:SZ"
    df[!, :collection_time] = DateTime.(df[:, :collection_time], date_format)

    sender |> @source(df = df, table = :readings, symbols = [:city, :make], at = :collection_time)

    records = "readings,city=London,make=Omron humidity=0.334,temperature=29.4 1681132171000000000\n"
    records *= "readings,city=Nairobi,make=Honeywell humidity=0.51,temperature=24.0 1681132182000000000\n"

    @test sender.buffer == records
end


@testset "QuestDBOperators empty table name exception" begin
    sender = Sender()

    @test_throws DomainError table(sender, "")
end

@testset "QuestDBTypes empty table name exception" begin
    sender = Sender()

    @test_throws DomainError sender |> @table("")
end


@testset "QuestDBOperators illegal character in table name exception" begin
    sender = Sender()

    @test_throws QuestDBExceptions.IllegalTableNameCharacterException table(sender, "tab?le")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException table(sender, "tab.le:")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException table(sender, "~tab+le")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException table(sender, "(my**table)")
end

@testset "QuestDBTypes illegal character in table name exception" begin
    sender = Sender()

    @test_throws QuestDBExceptions.IllegalTableNameCharacterException sender |> @table("tab?le")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException sender |> @table( "tab.le:")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException sender |> @table("~tab+le")
    @test_throws QuestDBExceptions.IllegalTableNameCharacterException sender |> @table("(my**table)")
end

@testset "QuestDBOperators multiple table definitions exception" begin
    sender = Sender()

    sender |> x -> table(x, :table) |> x -> symbol(x, :sym => :P)
    @test_throws QuestDBExceptions.MultipleTableDefinitionsException sender |> x -> table(x, :table) |> 
                                                                               x -> IntegerColumn(x -> :count => 13)
                                                
end

@testset "QuestDBTypes  multiple table definitions exception" begin
    sender = Sender()

    sender |> @table(:table) |> @symbol(:sym => :P)
    @test_throws QuestDBExceptions.MultipleTableDefinitionsException sender |> @table(:table) |> @IntegerColumn(:count => 13)
end

@testset "QuestDBOperators empty column name exception" begin
    sender = Sender()

    sender = Sender()
    @test_throws QuestDBExceptions.EmptyColumnNameException sender |> x -> table(x, "table") |> x -> IntegerColumn(x, "" => 15)
end

@testset "QuestDBTypes empty column name exception" begin
    sender = Sender()

    sender = Sender()
    @test_throws QuestDBExceptions.EmptyColumnNameException sender |> @table("table") |> @IntegerColumn("" => 15)
end

@testset "QuestDBOperators illegal character column name exception" begin
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> x -> table(x, "table") |> x -> symbol(x, "?make" => :Omron)
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> x -> table(x, "table") |> x -> FloatColumn(x, "am~ount" => 15.989)
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> x -> table(x, "table") |> x -> StringColumn(x, "ip*" => "127.0.0.1")
end

@testset "QuestDBTypes illegal character column name exception" begin
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> @table("table") |> @symbol("?make" => :Omron)
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> @table("table") |> @FloatColumn("am~ount" => 15.989)
    sender = Sender()
    @test_throws QuestDBExceptions.IllegalColumnNameCharacterException sender |> @table("table") |> @StringColumn("ip*" => "127.0.0.1")
end

@testset "QuestDBOperators missing table declaration exception" begin
    sender = Sender()

    @test_throws QuestDBExceptions.MissingTableDeclarationException IntegerColumn(sender, :age => 20)
    @test_throws QuestDBExceptions.MissingTableDeclarationException CharColumn(sender, :state => 'A')
    @test_throws QuestDBExceptions.MissingTableDeclarationException FloatColumn(sender, :weight => 70.2)
    @test_throws QuestDBExceptions.MissingTableDeclarationException symbol(sender, :city => :Mombasa)
end

@testset "QuestDBTypes missing table declaration exception" begin
    sender = Sender()

    @test_throws QuestDBExceptions.MissingTableDeclarationException sender |> @IntegerColumn(:age => 20)
    @test_throws QuestDBExceptions.MissingTableDeclarationException sender |> @CharColumn(:state => 'A')
    @test_throws QuestDBExceptions.MissingTableDeclarationException sender |> @FloatColumn(:weight => 70.2)
    @test_throws QuestDBExceptions.MissingTableDeclarationException sender |> @symbol(:city => :Mombasa)
end


@testset "QuestDBOperators unsupported dataframe column type exception" begin
    sender  = Sender()

    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0], 
                humidity=[missing, missing])
    
    @test_throws QuestDBExceptions.UnsupportedColumnTypeException Source(sender, df, :table, symbols = [:city, :make])
end

@testset "QuestDBTypes unsupported dataframe column type exception" begin
    sender  = Sender()

    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0], 
                humidity=[missing, missing])
    
    @test_throws QuestDBExceptions.UnsupportedColumnTypeException sender |> @source(df = df, table = :readings, 
                                                                                    symbols = [:city, :make])
end

@testset "QuestDBOperators unsupported at type exception" begin
    sender = Sender()
    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0],
                humidity=[0.54, 0.34],
                collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])
    
    @test_throws QuestDBExceptions.UnsupportedAtColumnTypeException Source(sender, df, :table, symbols = [:city, :make], at = :collection_time)
end

@testset "QuestDBTypes unsupported at type exception" begin
    sender = Sender()
    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0],
                humidity=[0.54, 0.34],
                collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])
    
    @test_throws QuestDBExceptions.UnsupportedAtColumnTypeException sender |> @source(df = df, table = :readings, at = :collection_time,
                                                                                      symbols = [:city, :make])
end

@testset "QuestDBOperators method error" begin
    sender = Sender()
    @test_throws MethodError sender |> x -> table(x, :table) |> x -> IntegerColumn(x, :age => "15")
    sender = Sender()
    @test_throws MethodError sender |> x -> table(x, :table) |> x -> StringColumn(x, :address => 15656)
    sender = Sender()
    @test_throws MethodError sender |> x -> table(x, :table) |> x -> FloatColumn(x, :group => 'A')
    sender = Sender()
    @test_throws MethodError sender |> x -> table(x, :table) |> x -> CharColumn(x, :label => "15")
    sender = Sender()
    @test_throws MethodError sender |> x -> table(x, :table) |> x -> symbol(x, :sym => 789)
end


@testset "QuestDBTypes method error" begin
    sender = Sender()
    @test_throws MethodError sender |> @table(:table) |> @IntegerColumn(:age => "15")
    sender = Sender()
    @test_throws MethodError sender |> @table(:table) |> @StringColumn(:address => 15656)
    sender = Sender()
    @test_throws MethodError sender |> @table(table) |> @FloatColumn(:group => 'A')
    sender = Sender()
    @test_throws MethodError sender |> @table(:table) |> @CharColumn(:label => "15")
    sender = Sender()
    @test_throws MethodError sender |> @table(:table) |> @symbol(:sym => 789)
end




#########################################################
#                                                       #
#                                                       #
#            SERVER TESTS (INTEGRATION)                 #
#                                                       #
#                                                       #
#########################################################

"""

The tests depend on the existence of the following table in the local QuestDB Server

CREATE TABLE quest_db_client_jl (
  timestamp TIMESTAMP,
  city SYMBOL,
  temperature DOUBLE,
  humidity DOUBLE,
  make SYMBOL
) TIMESTAMP(timestamp) PARTITION BY DAY;

"""

@testset "QuestDBSender server connection" begin
    sender = Sender()

    @test isnothing(QuestDBSender.connect(sender))

    QuestDBSender.close(sender)
end


@testset "QuestDBSender basic server writes" begin
    sender = Sender()

    ## Connect to the sender
    QuestDBSender.connect(sender)

    sender |> 
        x -> table(x, :quest_db_client_jl) |> 
        x -> symbol(x, :make => :Omron) |>
        x -> symbol(x, :city => :Nairobi) |>
        x -> FloatColumn(x, :temperature => 26.8) |> 
        x -> FloatColumn(x, :humidity => 0.51) |>
        x -> At(x, DateTime(2023, 4, 1))
    
    records = "quest_db_client_jl,make=Omron,city=Nairobi temperature=26.8,humidity=0.51 1680307200000000000\n"

    sender |> 
        x -> table(x, :quest_db_client_jl) |> 
        x -> symbol(x, :make => :Honeywell) |> 
        x -> symbol(x, :city => :London) |>
        x -> FloatColumn(x, :temperature => 22.9) |>
        x -> FloatColumn(x, :humidity => 0.254) |>
        x -> At(x, DateTime(2023, 4, 2))
    
    
    records *= "quest_db_client_jl,make=Honeywell,city=London temperature=22.9,humidity=0.254 1680393600000000000\n"

    @testset "QuestDBOperators writes" begin
        ## Attempted write does not persist as data is < buffer size
        @test isnothing(QuestDBSender.write(sender))
        @test sender.buffer == records
        @test sender.hasTable == false
        @test sender.hasFields == false

        ## Flush the data
        @test isnothing(QuestDBSender.flush(sender))
        @test isempty(sender.buffer)
        @test sender.hasTable == false
        @test sender.hasFields == false
    end


    records = ""

    sender |>
        @table(:quest_db_client_jl) |>
        @symbol(:make => :Omron) |>
        @symbol(:city => :Nairobi) |>
        @FloatColumn(:temperature => 26.8) |>
        @FloatColumn(:humidity => 0.334) |>
        @At(DateTime(2023, 4, 1))

    records = "quest_db_client_jl,make=Omron,city=Nairobi temperature=26.8,humidity=0.334 1680307200000000000\n"

    sender |>
        @table(:quest_db_client_jl) |>
        @symbol(:make => :Honeywell) |>
        @symbol(:city => :Kisumu) |>
        @FloatColumn(:temperature => 30.2) |>
        @FloatColumn(:humidity => 0.54) |>
        @At(DateTime(2023, 4, 2))
    
    records *= "quest_db_client_jl,make=Honeywell,city=Kisumu temperature=30.2,humidity=0.54 1680393600000000000\n"
    
    @testset "QuestDBTypes writes" begin
      ## Attempted write does not persist as data is < buffer size
      @test isnothing(QuestDBSender.write(sender))
      @test sender.buffer == records
      @test sender.hasTable == false
      @test sender.hasFields == false

      ## Flush the data
      @test isnothing(QuestDBSender.flush(sender))
      @test isempty(sender.buffer)
      @test sender.hasTable == false
      @test sender.hasFields == false
    end

    ## Close the socket
    QuestDBSender.close(sender)
end


@testset "QuestDBSender dataframe writes" begin
    sender = Sender()

    ## Connect to the sender
    QuestDBSender.connect(sender)

    ## DataFrame instance
    df = DataFrame(city=["London", "Nairobi"], 
                make=[:Omron, :Honeywell], 
                temperature=[29.4, 24.0], 
                humidity=[0.334, 0.51],
                collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"])

    ## Cast the collection_time to DateTime
    date_format = dateformat"y-m-dTH:M:SZ"
    df[!, :collection_time] = DateTime.(df[:, :collection_time], date_format)
    
    sender |> x -> Source(x, df, :quest_db_client_jl, symbols = [:city, :make], at = :collection_time)

    records = "quest_db_client_jl,city=London,make=Omron humidity=0.334,temperature=29.4 1681132171000000000\n"
    records *= "quest_db_client_jl,city=Nairobi,make=Honeywell humidity=0.51,temperature=24.0 1681132182000000000\n"

    @testset "QuestDBOperators writes" begin
        ## Attempt to write, but fail as the buffer isn't full
        @test isnothing(QuestDBSender.write(sender))
        @test sender.buffer == records
        @test sender.hasTable == false
        @test sender.hasFields == false

        ## Flush the data
        @test isnothing(QuestDBSender.flush(sender))
        @test isempty(sender.buffer)
        @test sender.hasTable == false
        @test sender.hasFields == false
    end
    
    sender |> @source(df = df, table = :quest_db_client_jl, symbols = [:city, :make], at = :collection_time)

    @testset "QuestDBTypes writes" begin
        ## Attempt to write, but fail as the buffer isn't full
        @test isnothing(QuestDBSender.write(sender))
        @test sender.buffer == records
        @test sender.hasTable == false
        @test sender.hasFields == false

        ## Flush the data
        @test isnothing(QuestDBSender.flush(sender))
        @test isempty(sender.buffer)
        @test sender.hasTable == false
        @test sender.hasFields == false
    end

    ## Close the sender
    QuestDBSender.close(sender)
end


@testset "QuestDBSender clear" begin
    sender = Sender()

    sender |> 
        x -> table(x, :quest_db_client_jl) |> 
        x -> symbol(x, :make => :Omron) |>
        x -> symbol(x, :city => :Nairobi) |>
        x -> FloatColumn(x, :temperature => 26.8) |> 
        x -> FloatColumn(x, :humidity => 0.51) |>
        x -> At(x, DateTime(2023, 4, 1))
    
    records = "quest_db_client_jl,make=Omron,city=Nairobi temperature=26.8,humidity=0.51 1680307200000000000\n"
    
    @test sender.buffer == records
    @test isnothing(QuestDBSender.clear(sender))
    @test isempty(sender.buffer)
end

## Test that close returns nothing
@testset "QuestDBSender close" begin
    sender = Sender()

    QuestDBSender.connect(sender)

    @test isnothing(QuestDBSender.close(sender))
end

