"""
QuestDB Operators

Functional Implementation of the QuestDBClient interface
"""

module QuestDBOperators

using ..QuestDBSender
using ..QuestDBExceptions

using Dates
using UUIDs
using DataFrames

export table, symbol, IntegerColumn, FloatColumn, BoolColumn, StringColumn, CharColumn, DateTimeColumn, DateColumn,
       UUIDColumn, At, AtNow, Source

"""
const ColumnName = Union{Symbol, String}

The column name can be specified as a symbol or as a string
"""
const ColumnName = Union{Symbol, String}


"""
    const SymbolColumnValue = Union{Symbol, String}

A symbol column can take values that are either a Symbol or a String
"""
const SymbolColumnValue = Union{Symbol, String, Nothing}

"""
    const supportedTypes::Vector{Type} = [Integer, AbstractFloat, AbstractString, Symbol, Dates.DateTime, 
                                         Dates.Date, Char, UUID]
A list of supported types
"""
const supportedTypes = [Integer, AbstractFloat, AbstractString, Symbol, 
                        Dates.DateTime, Dates.Date, Char, UUID]

"""
Constant Definitions
"""

"""
    const COMMA = ","

A constant referencing a comma - ','
"""
const COMMA = ','

"""
    const SPACE_CHARACTER = ' '

A constant referencing the space character
"""
const SPACE_CHARACTER = ' '

"""
    const RETURN_CHARACTER = '\\n'

A constant referencing the return character
"""
const RETURN_CHARACTER = '\n'

"""
    const EQUALS_CHARACTER = '='

A constant referencing the equals character.
"""
const EQUALS_CHARACTER = '='


"""
    const UNSUPPORTED_CHARACTERS = ['?', '.' , ',' , '\'' , '"' , '\\' , '/', ':' , '(' , ')' , '+' , '-' , '*' , '%' , '~' , ' ' , '\0']

The list of unsupported characters for table & column names
"""
const UNSUPPORTED_CHARACTERS = ['?', '.' , ',' , '\'' , '"' , '\\' , '/', ':' , '(' , ')' , '+' , '-' , '*' , '%' , '~' , ' ' , '\0']


"""

    table(sender::Sender, name::T)::Sender where {T <: ColumnName}

Table definition function for an ILP entry. Adds a table name to the `sender`'s buffer and returns the sender with
an updated buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> table(sender, :readings)
## sender with table readings
```
"""
function table(sender::Sender, name::T)::Sender where {T <: ColumnName}
    name = string(name)
    length(name) == 0 && throw(DomainError("table name cannot be empty"))
    ## Assert that all characters are supported
    checkUnsupportedCharacters(name, IllegalTableNameCharacterException)
    ## Check if a table has already been defined
    sender.hasTable && throw(MultipleTableDefinitionsException("cannot define table more than once."))
    ## Add the table to the buffer
    sender.buffer = sender.buffer * string(name) * COMMA
    sender.hasTable = true
    return sender
end


"""
    symbol(sender::Sender, symbol::Pair{T, V})::Sender where {T <: ColumnName, V <: SymbolColumnValue}

Symbol definition function for an ILP entry. Adds a symbol (tag_set) to the `sender`'s buffer and returns the sender with
an updated buffer. The `symbol` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: SymbolColumnValue`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

The `SymbolColumnValue` is `Union{Symbol, String}`

# Example
```julia-repl
julia> symbol(sender, :make => :Omron)
## sender with symbol make=Omron
```
"""
function symbol(sender::Sender, symbol::Pair{T, V})::Sender where {T <: ColumnName, V <: SymbolColumnValue}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define symbol before a table is defined."))
    sender.hasFields && throw(MalformedLineProtocolSyntaxException("cannot define symbol after field(s)."))
    ## Validate symbol characters
    
    ## If the last character of the buffer is a comma, then the buffer contains a table only and this is the first symbol
    ## If the last character of the buffer is space, then the buffer contains at least one other symbol
    lastBufferChar = sender.buffer[end]

    if lastBufferChar == SPACE_CHARACTER
        ## Trim the last character (space character) and append a comma
        sender.buffer = sender.buffer[1:end - 1] * COMMA
        lastBufferChar = COMMA
    end
        
    if lastBufferChar == COMMA
        sender.buffer = writeSymbol(sender.buffer, symbol)       
    else
        throw(MalformedLineProtocolSyntaxException("malformed line protocol syntax detected: $(sender.buffer)"))
    end

    return sender
end


"""
    
    writeSymbol(queryString::String, symbol::Pair{T, V})::String where {T <: ColumnName, V <: SymbolColumnValue}

Function used to create append a symbol when constructing an ILP string. 
The `symbol` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: SymbolColumnValue`

Also asserts that QuestDB unsupported column name characters result in exception propagation

The `ColumnName` is `Union{Symbol, String}`

The `SymbolColumnValue` is `Union{Symbol, String}`

# Example
```julia-repl
julia> writeSymbol("", :make => :Omron)
make=Omron 
```   
"""
function writeSymbol(queryString::String, symbol::Pair{T, V})::String where {T <: ColumnName, V <: SymbolColumnValue}
    ## If the column name is emtpy, throw an error
    if length(strip(string(symbol.first))) == 0 throw(EmptyColumnNameException("column name cannot be empty")) end
    ## If symbol value is nothing, return an empty string
    if isnothing(symbol.second) return "" end
    ## Asset that all characters in the column name are not illegal
    checkUnsupportedCharacters(string(symbol.first), IllegalColumnNameCharacterException)

    return queryString * string(symbol.first) * EQUALS_CHARACTER * string(symbol.second) * SPACE_CHARACTER
end


"""

    IntegerColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Integer, Nothing}}

IntegerColumn definition function for an ILP entry. Adds a field of type integer to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Integer, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

All `Integer` subtypes are supported:
    `Bool`, `BigInt`, `Int128`, `Int64`, `Int32`, `Int16`, `Int8`,
    `UInt128`, `UInt64`, `UInt32`, `UInt16`, `UInt8`

# Example
```julia-repl
julia> IntegerColumn(sender, :count => 12)
## sender with field count=12
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> IntegerColumn(sender, :count => nothing)
## sender without an updated buffer
```
"""
function IntegerColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Integer, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define integer column before a table is defined."))

    ## If the sender has a fields already, prepend a comma, else, just append the column to the ILP
    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    sender.buffer  = writeFieldColumn(sender.buffer, data)

    ## Mark the sender as having a field
    sender.hasFields = true

    return sender
end

"""
    writeFieldColumn(queryString::String, data::Pair{T, V})::String where {T <:ColumnName, V <: Any}

Function used to append any other field to the ILP entry. If the type of `V` is `nothing`, the passed field will not be written
to the `queryString` which is eventually appended to the `sender`'s buffer.

A check for unsupported column characters is also performed.

# Example
```julia-repl
julia> writeFieldColumn(sender, :count => 15)
## sender with field count=12
```

# Example
```julia-repl
julia> IntegerColumn(sender, :count => nothing)
## sender without an updated buffer
```
"""
function writeFieldColumn(queryString::String, data::Pair{T, V})::String where {T <:ColumnName, V <: Any}
    ## If the column name is emtpy, throw an error
    if length(strip(string(data.first))) == 0 throw(EmptyColumnNameException("column name cannot be empty")) end
    ## If the value is nothing, don't write anything
    if isnothing(data.second) return "" end
    ## Assert that all characters in the column name are not illegal
    checkUnsupportedCharacters(string(data.first), IllegalColumnNameCharacterException)

    return queryString * string(data.first) * EQUALS_CHARACTER * string(data.second)
end

"""
    checkUnsupportedCharacters(subject::T, exception::Type{E}) where {T <: ColumnName, E <: QuestDBClientException}

Asserts that only supported column name characters pass this evaluation. Any unsupported character results in throwing a
`QuestDBClientException`.

Unsupported characters include:
    ['?', '.' , ',' , '\'' , '"' , '\\' , '/', ':' , '(' , ')' , '+' , '-' , '*' , '%' , '~' , ' ' , '\0']
"""
function checkUnsupportedCharacters(subject::T, exception::Type{E}) where {T <: ColumnName, E <: QuestDBClientException}
    ## Check if the table name has characters that are unsupported
    matched_char_flags = contains.(subject, UNSUPPORTED_CHARACTERS)
    ## Means unsupported characters have been detected
    if in(1, matched_char_flags)
        matched_indices = findall(==(1), matched_char_flags)
        illegal_chars = join(UNSUPPORTED_CHARACTERS[matched_indices], ", ")
        throw(exception("unsupported character(s) $(illegal_chars)  detected in your specified table name: $(subject)"))
    end
end

"""
    BoolColumn(sender::Sender, data::Pair{T, Bool})::Sender where {T <: ColumnName}

BoolColumn definition function for an ILP entry. Adds a field of type bool to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, Bool}` where `T <: ColumnName`.

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> BoolColumn(sender, :present => true)
## sender with field present=true
```
"""
function BoolColumn(sender::Sender, data::Pair{T, Bool})::Sender where {T <: ColumnName}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define boolean column before a table is defined."))

    return IntegerColumn(sender, data)
end


"""
    FloatColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{AbstractFloat, Nothing}}

FloatColumn definition function for an ILP entry. Adds a field of type float to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{AbstractFloat, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

All `AbstractFloat` subtypes are supported:
    `BigFloat`, `Float64`, `Float32`, `Float16`

# Example
```julia-repl
julia> FloatColumn(sender, :tempareture => 29.4)
## sender with field tempareture=29.4
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> FloatColumn(sender, :tempareture => nothing)
## sender without an updated buffer
```
"""
function FloatColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{AbstractFloat, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define float column before a table is defined."))

    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    sender.buffer = writeFieldColumn(sender.buffer, data)
    
    sender.hasFields = true

    return sender
end

"""
    StringColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{AbstractString, Nothing}}

StringColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{AbstractString, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

All `AbstractString` subtypes are supported:
    `Core.Compiler.LazyString`, `InlineStrings.InlineString`, `LaTeXStrings.LaTeXString`, `LazyString`,
    `String`, `SubString`, `SubstitutionString`, `Test.GenericString`

# Example
```julia-repl
julia> StringColumn(sender, :city => "Nairobi")
## sender with field city=Nairobi
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> StringColumn(sender, :city => nothing)
## sender without an updated buffer
```
"""
function StringColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{AbstractString, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define string column before a table is defined."))

    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    sender.buffer = writeFieldColumn(sender.buffer, data)

    sender.hasFields = true

    return sender
end

"""
    CharColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Char, Nothing}}

CharColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Char, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> CharColumn(sender, :region => 'A')
## sender with field region=A
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> CharColumn(sender, :region => nothing)
## sender without an updated buffer
```
"""
function CharColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Char, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define char column before a table is defined."))

    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    sender.buffer = writeFieldColumn(sender.buffer, data)

    sender.hasFields = true

    return sender
end

"""
    DateTimeColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{DateTime, Nothing}}

DateTimeColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{DateTime, Nothing}`

The DateTime is converted to milliseconds since UNIXEPOCH

This is not the record's designated timestamp field but another field whose value is a timestamp.

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> DateTimeColumn(sender, :pick_up_date => now())
## sender with field pick_up_date=1680990219992
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> DateTimeColumn(sender, :pick_up_date => nothing)
## sender without an updated buffer
```
"""
function DateTimeColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{DateTime, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define datetime column before a table is defined."))

    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    ## Get the time in milliseconds
    _timeInMilliseconds = ifelse(isnothing(data.second), nothing, Dates.value(data.second) - Dates.UNIXEPOCH)
    sender.buffer = writeFieldColumn(sender.buffer, data.first => _timeInMilliseconds)

    sender.hasFields = true
    
    return sender
end

"""
    DateColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Date, Nothing}}

DateColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{Date, Nothing}`

The Date is converted to milliseconds since UNIXEPOCH

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> DateColumn(sender, :collection_date => Date(2023, 4, 8))
## sender with field collection_date=1680912000000
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> DateColumn(sender, :collection_date => nothing)
## sender without an updated buffer
```
"""
function DateColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{Date, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define date column before a table is defined."))

    _timeInMilliseconds = nothing
    ## Get the day from the dates param
    if(!isnothing(data.second))
        _timeInMilliseconds = Dates.value(DateTime(data.second)) - Dates.UNIXEPOCH
    end

    sender.buffer = writeFieldColumn(sender.buffer, data.first => _timeInMilliseconds)

    sender.hasFields = true

    return sender
end



"""
    UUIDColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{UUID, Nothing}}

UUIDColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{UUID, Nothing}`

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> using UUIDs
julia> using Random
julia> rng = MersenneTwister(1234);
julia> u4 = uuid4(rng);
julia> UUIDColumn(sender, :user_id => u4)
## sender with field user_id=7a052949-c101-4ca3-9a7e-43a2532b2fa8
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> UUIDColumn(sender, :user_id => nothing)
## sender without an updated buffer
```
"""
function UUIDColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{UUID, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define UUID column before a table is defined."))

    if sender.hasFields
        sender.buffer = sender.buffer * COMMA
    end

    sender.buffer = writeFieldColumn(sender.buffer, data)

    sender.hasFields = true

    return sender
end

"""
    UUIDColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{String, Nothing}}

UUIDColumn definition function for an ILP entry. Adds a field of type string to the `sender`'s buffer and returns the sender with
an updated buffer. The `data` argument is a `Pair{T, V}` where `T <: ColumnName` and `V <: Union{String, Nothing}`

Takes in the UUID as a string.

This requires that a `table` has already been added to the sender's buffer.

The `ColumnName` is `Union{Symbol, String}`

# Example
```julia-repl
julia> UUIDColumn(sender, :user_id => "7a052949-c101-4ca3-9a7e-43a2532b2fa8")
## sender with field user_id=7a052949-c101-4ca3-9a7e-43a2532b2fa8
```

If `nothing` is passed as the second part of the part of the data pair, `V`, this column won't be written
# Example
```julia-repl
julia> UUIDColumn(sender, :user_id => nothing)
## sender without an updated buffer
```
"""
function UUIDColumn(sender::Sender, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Union{String, Nothing}}
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define UUID column before a table is defined."))

    return StringColumn(sender, data)
end


"""
    At(sender::Sender, timestamp::DateTime)::Nothing

At column definition function for an ILP entry. This is the designated timestamp field.

The timestamp is converted to nanoseconds since UNIXEPOCH

This requires that a `table` has already been added to the sender's buffer.

Upon setting this field, the `hasFields` and `hasTable` properties of the `sender` are set to false. This also marks the 
end of the record with a '\\n'.

Serves as a terminal definition of a record. Should always be defined last.

!!! note

    The `sender` attempts to write values to the `QuestDB Database Server` depending
    on whether the buffer size has been met or exceeded when `At` is executed.

# Example
```julia-repl
julia> At(sender, now())
## sender with field 1680993284179000000\\n
```
"""
function At(sender::Sender, timestamp::DateTime)::Nothing
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define At column before a table is defined."))\

    if sender.hasFields
        sender.buffer = sender.buffer * SPACE_CHARACTER
    end
    
    ## Convert the datetime to nano seconds
    time_ns = convert(Dates.Nanosecond, Dates.Millisecond(Dates.value(timestamp) - Dates.UNIXEPOCH))
    ## Multiply the time by 1_000_000 to convert it to Nanoseconds
    sender.buffer = sender.buffer * SPACE_CHARACTER * string(time_ns.value) * RETURN_CHARACTER

    ## Mark the hasFields to false & hasTable to false
    sender.hasFields = false
    sender.hasTable = false

    ## Persist to QuestDB
    QuestDBSender.write(sender)

    return nothing
end

"""
    AtNow(sender::Sender)::Nothing

This requires that a `table` has already been added to the sender's buffer.

Resolves to:
    At(sender, now())

!!! note

    The `sender` attempts to write values to the `QuestDB Database Server` depending
    on whether the buffer size has been met or exceeded when `AtNow(sender)` is executed.

# Example
```julia-repl
julia> AtNow(sender)
## sender with field 1680993284179000000\\n
```
"""
function AtNow(sender::Sender)::Nothing
    !sender.hasTable && throw(MissingTableDeclarationException("cannot define AtNow column before a table is defined."))

    return At(sender, now())
end

"""
    Source(sender::Sender, df::DataFrame, table::TT; 
           at::T = "", symbols::Vector{V} = [])::Sender where {TT<: ColumnName, T <: ColumnName, V <: ColumnName}

Takes in a `DataFrame` object and creates ILP insert statement for each row element. 

# Arguments
    - `sender::Sender` : QUestDBClient sender object
    - `df::DataFrame`: the `DataFrame` that serves as the source of the data
    - `table::TT where {TT <: ColumnName}` : the name of the `table`
    - `at::T where {T <: ColumnName}` : the column that has timestamp values that server as the designated timestamp
    - `symbols::Vector{V} where {V <: ColumnName}`: the list of column names whose columns serve as `tag_set` values for an ILP record

!!! note

    The `sender`, `df`, and `table` arguments are compulsory and are positional arguments.
    The `at` and `symbols` arguments are optional named arguments.

The `ColumnName` is `Union{Symbol, String}`
    
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
julia> Source(sender, df, :readings, symbols=[:city, :make]);
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
julia> Source(sender, df, :readings, symbols = [:city, :make], at = :collection_time);
## sender with 2 ILP records from the 2 rows in the DataFrame
```

!!! note

    The `sender` attempts to write values to the `QuestDB Database Server` depending
    on whether the buffer size has been met or exceeded while reading the rows of the 
    `DataFrame`. This is even before the `flush` or `close` function is called.

"""
function Source(sender::Sender, df::DataFrame, table::TT;
                at::T = "", symbols::Vector{V} = [])::Sender where {TT<: ColumnName, T <: ColumnName, V <: ColumnName}
    ## Create a columnName => columnType dictionary
    columnsDict::Dict{String, DataType} = Dict(names(df) .=> eltype.(eachcol(df)))
    unsupportedTypes = []

    ## Assert that all types are supported or request for a cast
    for(k, v) in columnsDict
        if !in(1, v .<: supportedTypes)
            push!(unsupportedTypes, k => v)
        end
    end

    ## Log unsupported types
    if !isempty(unsupportedTypes)
        throw(UnsupportedColumnTypeException("detected unsupported column type(s): $unsupportedTypes. Cast them to a supported type."))
    end

    hasAt = false
    at = string(at)
    ## Assert the at column exists
    if length(at) != 0
        ## Assert the column is a Timestamp type
        if(haskey(columnsDict, at))
            type = columnsDict[at]

            if(!(type <: Dates.DateTime))
                throw(UnsupportedAtColumnTypeException("specified At column: $at of type $type is not of type Timestamp"))
            end
            hasAt = true
        end
    end

    ## Handle Missing type

    ## Remove the columns from the columnsDict
    namedCols = string.([symbols...])
    ifelse(hasAt, push!(namedCols, at), nothing)

    # namedCols = string.([namedCols])

    for namedCol in namedCols
        delete!(columnsDict, namedCol)
    end

    ## Loop through each row building the ILP String as guided by the types
    for row in eachrow(df)
        ## Write the table first
        sender = QuestDBOperators.table(sender, table)
        ## Write symbols first
        for s in symbols
            sender = symbol(sender, s => row[s])
        end

        ## Loop through other columns & write them
        for (col, coltype) in columnsDict
            sender = writeRowEnty!(sender, coltype, col => row[col])
        end

        ## If the at was specified, write it last, else write AtNow
        if hasAt
            At(sender, row[at])
        else
            AtNow(sender)
        end
    end

    ## Return the sender
    return sender
end

"""

    writeRowEnty!(sender::Sender, dataType::Type, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Any}

A helper function to build ILP records from a dataframe based on the column types.
"""
function writeRowEnty!(sender::Sender, dataType::Type, data::Pair{T, V})::Sender where {T <: ColumnName, V <: Any}
    if dataType <: Symbol
        return symbol(sender, data.first => Symbol(data.second))
    elseif dataType <: Integer
        return IntegerColumn(sender, data)
    elseif dataType <: AbstractFloat
        return FloatColumn(sender, data)
    elseif dataType <: AbstractString
        return StringColumn(sender, data)
    elseif dataType <: DateTime
        return DateTimeColumn(sender, data)
    elseif dataType <: Date
        return DateColumn(sender, data)
    elseif dataType <: Char
        return CharColumn(sender, data)
    elseif dataType <: UUID
        return UUIDColumn(sender, data)
    else
        throw(UnsupportedColumnTypeException("column type of $dataType is not supported"))
    end
end

end
