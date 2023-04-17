"""
Macro Definitions of supported types

Type macros return a sender object
"""

module QuestDBTypes
import ..QuestDBOperators

export @table, @symbol, @IntegerColumn, @StringColumn, @CharColumn, @DateTimeColumn, @BoolColumn,
       @DateColumn, @UUIDColumn, @AtNow, @At, @FloatColumn, @source
import Base: string

"""

    @table(name)

@table definition macro for an ILP entry. Returns an expression that upon evaluation returns a closure that taken a `sender` as an argument. 
Once the closure is evaluated, it adds a table name to the `sender`'s buffer and returns the sender with an updated buffer.

The `name` should be of type `ColumnName`

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @table(:readings)
## sender with table readings
```
"""
macro table(name)
    return quote
        (sender) -> QuestDBOperators.table(sender, $(esc(name)))
    end
end


"""
    @symbol(symbol::Pair{T, V}) where {T <: ColumnName, V <: SymbolColumnValue}

@symbol definition macro for an ILP entry. Adds a symbol (tag_set) to the `sender`'s buffer and returns the closure that takes a `sender` as an argument.
Once the closure is evaluated, it adds a symbol to the `sender`'s buffer and returns the sender with an updated buffer. The `symbol` argument is a
`Pair{T, V}` where `T <: ColumnName` and `V <: SymbolColumnValue`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

The `SymbolColumnValue` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @symbol(:make => :Omron)
## sender with symbol make=Omron
```
"""
macro symbol(symbolPair)
    return quote
        (sender) -> QuestDBOperators.symbol(sender, $(esc(symbolPair)))
    end
end

"""

    @IntegerColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{Integer, Nothing}}

@IntegerColumn definition macro for an ILP entry. Adds a field of type integer to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, it returns the sender with an updated buffer.
The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Integer, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

The `SymbolColumnValue` is `Union{Symbol, String}`

All `Integer` subtypes are supported:
    `Bool`, `BigInt`, `Int128`, `Int64`, `Int32`, `Int16`, `Int8`,
    `UInt128`, `UInt64`, `UInt32`, `UInt16`, `UInt8`

# Example
```julia-repl
julia> sender |> @IntegerColumn(:count => 12)
## sender with field count=12
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @IntegerColumn(:count => nothing)
## sender without an updated buffer
```
"""
macro IntegerColumn(data)
    return quote
        (sender) -> QuestDBOperators.IntegerColumn(sender, $(esc(data)))
    end
end

"""
    BoolColumn(sender::Sender, data::Pair{T, Bool}) where {T <: ColumnName}

@BoolColumn definition macro for an ILP entry. Adds a field of type bool to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 
The `data` argument is a `Pair{T, Bool}` where `T <: ColumnName`.

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @BoolColumn(:present => true)
## sender with field present=true
```
"""
macro BoolColumn(data)
    return quote
        (sender) -> QuestDBOperators.BoolColumn(sender, $(esc(data)))
    end
end


"""
    @FloatColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{AbstractFloat, Nothing}}

@FloatColumn definition function for an ILP entry. Adds a field of type float to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 

The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{AbstractFloat, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

All `AbstractFloat` subtypes are supported:
    `BigFloat`, `Float64`, `Float32`, `Float16`

# Example
```julia-repl
julia> sender |> @FloatColumn(:tempareture => 29.4)
## sender with field tempareture=29.4
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @FloatColumn(:tempareture => nothing)
## sender without an updated buffer
```
"""
macro FloatColumn(data)
    return quote
        (sender) -> QuestDBOperators.FloatColumn(sender, $(esc(data)))
    end
end


"""
    @StringColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{AbstractString, Nothing}}

@StringColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 

The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{AbstractString, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

All `AbstractString` subtypes are supported:
    `Core.Compiler.LazyString`, `InlineStrings.InlineString`, `LaTeXStrings.LaTeXString`, `LazyString`,
    `String`, `SubString`, `SubstitutionString`, `Test.GenericString`

# Example
```julia-repl
julia> sender |> @StringColumn(:city => "Nairobi")
## sender with field city=Nairobi
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @StringColumn(:city => nothing)
## sender without an updated buffer
```
"""
macro StringColumn(data)
    return quote
        (sender) -> QuestDBOperators.StringColumn(sender, $(esc(data)))
    end
end

"""
    @CharColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{Char, Nothing}}

@CharColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 

The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Char, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @CharColumn(:region => 'A')
## sender with field region=A
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @CharColumn(:region => nothing)
## sender without an updated buffer
```
"""
macro CharColumn(data)
    return quote
        (sender) -> QuestDBOperators.CharColumn(sender, $(esc(data)))
    end
end


"""
    @DateTimeColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{DateTime, Nothing}}

@DateTimeColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 
The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{DateTime, Nothing}`

The DateTime is converted to milliseconds since UNIXEPOCH

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @DateTimeColumn(:pick_up_date => now())
## sender with field pick_up_date=1680990219992
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @DateTimeColumn(:pick_up_date => nothing)
## sender without an updated buffer
```
"""
macro DateTimeColumn(data)
    return quote
        (sender) -> QuestDBOperators.DateTimeColumn(sender, $(esc(data)))
    end
end

"""
    @DateColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{Date, Nothing}}

@DateColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 
The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Date, Nothing}`

The Date is converted to milliseconds since UNIXEPOCH

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> sender |> @DateColumn(:collection_date => Date(2023, 4, 8))
## sender with field collection_date=1680912000000
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @DateColumn(:collection_date => nothing)
## sender without an updated buffer
```
"""
macro DateColumn(data)
    return quote
        (sender) -> QuestDBOperators.DateColumn(sender, $(esc(data)))
    end
end


"""
    @UUIDColumn(data::Pair{T, V}) where {T <: ColumnName, V <: Union{UUID, String, Nothing}}

@UUIDColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the closure that takes
a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 
The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{UUID, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> using UUIDs
julia> using Random
julia> rng = MersenneTwister(1234);
julia> u4 = uuid4(rng);
julia> sender |> @UUIDColumn(:user_id => u4)
## sender with field user_id=7a052949-c101-4ca3-9a7e-43a2532b2fa8
```

Works too when the passed UUID is a string

# Example
```julia-repl
julia> sender |> @UUIDColumn(:user_id => "7a052949-c101-4ca3-9a7e-43a2532b2fa8")
## sender with field user_id=7a052949-c101-4ca3-9a7e-43a2532b2fa8
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> sender |> @UUIDColumn(:user_id => nothing)
## sender without an updated buffer
```
"""
macro UUIDColumn(data)
    return quote
        (sender) -> QuestDBOperators.UUIDColumn(sender, $(esc(data)))
    end
end


"""
    @At(timestamp::DateTime)

@At column definition function for an ILP entry. This is the designated timestamp field.

The timestamp is converted to nanoseconds since UNIXEPOCH. 

It returns the closure that takes a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer. 

This requires that a `table` has already been added to the sender's buffer.

Upon setting this field, the `hasFields` and `hasTable` properties of the `sender` are set to false. This also marks the 
end of the record with a '\\n'. Furthermore, the `sender` attempts to write values to the `QuestDB Database Server` depending
on whether the buffer size has been met or exceeded.

Serves as a terminal definition of a record. Should always be defined last.

# Example
```julia-repl
julia> sender |> At(now())
## sender with field 1680993284179000000\\n
```
"""
macro At(data)
    return quote
        (sender) -> QuestDBOperators.At(sender, $(esc(data)))
    end
end


"""
    @AtNow

This requires that a `table` has already been added to the sender's buffer.

Resolves to:
    @At(now())

# Example
```julia-repl
julia> sender |> AtNow
## sender with field 1680993284179000000\\n
```
"""
macro AtNow()
    return quote
        (sender) -> QuestDBOperators.AtNow(sender)
    end
end

## Helps in conversion of QuoteNode to string
string(k::QuoteNode) = string(k.value)


"""
    @source(df::DataFrame = DataFrame(), table::TT = "",  at::T = "", 
            symbols::Vector{V} = []) where {TT<: ColumnName, T <: ColumnName, V <: ColumnName}

Takes in a `DataFrame` object and creates ILP insert statement for each row element.

This macro requires named arguments to be specified:
# Arguments
- `df::DataFrame`: the `DataFrame` that serves as the source of the data
- `table::TT where {TT <: ColumnName}` : the name of the `table`
- `at::T where {T <: ColumnName}` : the column that has timestamp values that server as the designated timestamp
- `symbols::Vector{V} where {V <: ColumnName}`: the list of column names whose columns serve as `tag_set` values for an ILP record

The `ColumnName` is `Union{Symbol, String}`

!!! note

    Only the `df` and `table` parameters must be specified. Then `at` and `symbols` parameters are optional.

It returns the closure that takes a `sender` as an argument. Once the closure is evaluated, returns the sender with an updated buffer.

The `table` specification is a requirement.


!!! note
    
    Supported column data types include:
        `Symbol`, `Integer` and subtypes, `AbstractFloat` and subtypes, `Bool`, `Char`, `AbstractString` and subtypes,
        `Date`, `DateTime`, `UUID`
    
    For `DataFrames`, entries of type `Missing` are not supported. They should be cast to `Nothing`.

`at` argument is used to specify the column header of the column in the `DataFrame` that will serve as the designated timestamp field. The column
should have values of type `DateTime` and will be converted to nanoseconds upon when converted to an ILP record. If the `at` is not specified,
the current time will be added to each ILP record.

`symbols` argument specifies a vector of columns headers of `DataFrame` columns that serve as the `tag_set` in the ILP statement. If `symbols`
are not specified, then no `tag_set` fields will be part of the ILP statement.


| city       | make       | tempareture  |   humidity  |
|:-----------|:-----------|-------------:|------------:|
| London     | Omron      |  29.4        |    0.334    |
| Nairobi    | Honeywell  |  24.0        |    0.51     |


- Assuming `df` below is the `DataFrame` above:

# Example
```julia-repl
julia> using DataFrames
julia> df = DataFrame(city=["London", "Nairobi"], make=[:Omron, :Honeywell], temperature=[29.4, 24.0], humidity=[0.334, 0.51]);
julia> sender |> source(df = df, table = :readings, symbols=[:city, :make]);
## sender with 2 ILP records from the 2 rows in the DataFrame
```

| city       | make       | tempareture  |   humidity  | collection_time          |
|:-----------|:-----------|-------------:|------------:|-------------------------:
| London     | Omron      |  29.4        |    0.334    | 2023-04-10T13:09:31Z     |
| Nairobi    | Honeywell  |  24.0        |    0.51     | 2023-04-10T13:09:42Z     |

- An example with the `at` field specified.

# Example
```julia-repl
julia> using DataFrames
julia> df = DataFrame(city=["London", "Nairobi"], make=[:Omron, :Honeywell], temperature=[29.4, 24.0], 
            humidity=[0.334, 0.51], collection_time=["2023-04-10T13:09:31Z", "2023-04-10T13:09:42Z"]);
julia> using Dates
julia> date_format = dateformat"y-m-dTH:M:SZ";
julia> df[!, :collection_time] = DateTime.(df[:, :collection_time], date_format);
julia> sender |> @source(df = df, table = :readings, symbols = [:city, :make], at = :collection_time);
## sender with 2 ILP records from the 2 rows in the DataFrame
```

!!! note

    The `sender` attempts to write values to the `QuestDB Database Server` depending
    on whether the buffer size has been met or exceeded while reading the rows of the 
    `DataFrame`. This is even before the `flush` or `close` function is called.

"""
macro source(args...)
    length(args) == 0 && throw(ArgumentError("no arguments have been passed"))
    (length(args) > 4) && throw(ArgumentError("this macro takes a maximum of 4 arguments. Number of arguments passed = $(length(args)): $(args)"))

    ## Get the keyword arguments
    local supportedKeywords = [:df, :table, :at, :symbols]
    df = nothing
    table = ""
    at = ""
    symbols = []

    for arg in args
        ## Assert the passed parameter is an expression
        if isa(arg, Expr)
            ## Asset the expression has the format (=, args)
            if !(arg.head == :(=))
                throw(ArgumentError("invalid argument format $(arg)"))
            end

            expr_args = arg.args
            (length(args) == 0 || length(args) < 2)&& throw(ArgumentError("invalid argument format $(arg)"))

            keyword = expr_args[1]           
            
            if keyword == :df
                df = expr_args[2]
            elseif keyword == :table
                table = expr_args[2]
            elseif keyword == :at
                at = expr_args[2]
            elseif keyword == :symbols
                ## This is expected to be a vector expression
                vect_exp = expr_args[2]
                symbols = string.(vect_exp.args)
            else
                ## Throw unsupported keyword error if the passed keyword isn't supported  
                throw(ArgumentError("unsupported keyword $(keyword) passed as an argument. Supported keywords are: $(supportedKeywords...)"))            
            end
        else
            ## Throw an error if the passed value is not an expression
            throw(ArgumentError("invalid argument format $(arg)"))
        end
    end

    ## Return a closure mapping the sender to QueryOperators.Source function
    return quote
        (sender) -> QuestDBOperators.Source(sender, $(esc(df)), $(esc(table)), at = $(esc(at)), symbols = $(esc(symbols)))
    end
end

end
