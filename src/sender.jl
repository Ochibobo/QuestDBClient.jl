"""
Create a client used to send the ilp data
"""

module QuestDBSender

using Parameters
using Sockets

export Sender, connect, close, write, flush
export Auth


"""
    const DEFAULT_BUFFER_SIZE::Int = 128 * 1024

Default buffer size of the sender's buffer
"""
const DEFAULT_BUFFER_SIZE::Int = 128 * 1024

"""
See: https://questdb.io/docs/reference/api/ilp/authenticate

Authentication object used
"""
@with_kw struct Auth
    kid::String
    dKey::String
    xKey::String
    yKey::String
end


"""
    @with_kw mutable struct Sender
        host::String="127.0.0.1"
        port::Int=9009
        batchSize::Int=DEFAULT_BUFFER_SIZE
        tlsMode::Bool=false
        hasTable::Bool=false
        hasFields::Bool=false
        auth::Union{Auth, Nothing} = nothing ## Can be changed into a new 
        buffer::String = ""
        socket::Union{TCPSocket, Nothing} = nothing
    end

`Sender` struct is the entity responsible for connecting to the `QuestDB Server`, build records & send them using the
ILP protocol. 

# Arguments
- `host::String` - the host address. The default one is the `localhost` or `127.0.0.1`
- `port::Int` - the port connected to on the host machine. Default value is `9009`
- `batchSize::Int` - the buffer size beyond which the contents of the buffer written to the server. Default size is 128 * 1024
- `hasTable::Bool` - used to indicate if an ILP record statement has a table defined
- `hasFields::Bool` - used to indicate if an ILP record statement has fields defined
- `buffer::String` - used to buffer ILP record statements before writing them to the server
- `socket::Union{TCPSocket, Nothing}` - holds the socket connection to the QuestDB Server instance
"""
@with_kw mutable struct Sender
    host::String="127.0.0.1"
    port::Int=9009
    batchSize::Int=DEFAULT_BUFFER_SIZE
    tlsMode::Bool=false
    hasTable::Bool=false
    hasFields::Bool=false
    auth::Union{Auth, Nothing} = nothing ## Can be changed into a new 
    buffer::String = ""
    socket::Union{TCPSocket, Nothing} = nothing
end

"""
Compare 2 auth objects
"""
function Base.:(==)(a::Auth, b::Auth)
    return true
end

"""
Compare 2 senders
"""
function Base.:(==)(a::Sender, b::Sender)
    return isequal(a.host, b.host) &&
          a.port == b.port &&
          a.batchSize == b.batchSize &&
          a.tlsMode == b.tlsMode &&
          a.hasTable == b.hasTable &&
          a.hasFields == b.hasFields &&
          a.auth == b.auth &&
          isequal(a.buffer, b.buffer)
          Sockets.getsockname(a) == Sockets.getsockname(b)
end
"""
    write(sender::Sender) 

Attempts to write the ILP record statements to the server. In case of an error, an exception is thrown
"""
function write(sender::Sender)
    try
        ## Only write to server when the buffer is full
        if length(sender.buffer) >= sender.batchSize
            ## Write to the server
            Base.write(sender.socket, sender.buffer)
            ## Clear the buffer
            clear(sender)
            ## Output on write
            @info "Inserted an ILP record..."
        end
    catch err
        @error "Failed to write to server\n"
        throw(err)
    end
end

function consume(sender::Sender)::Sender
    sender.auth = Auth("", "", "","")
    return sender
end

"""
    clear(sender::Sender)

Clears the buffer contents
"""
function clear(sender::Sender)
    sender.buffer = ""

    return nothing
end


"""
    connect(sender::Sender)

Attempts to connect the sender to the server socket. In case of an error, an exception is thrown
"""
function connect(sender::Sender)
    try
        sender.socket = Sockets.connect(sender.host, sender.port)
    catch err
        throw(err)
    end
    @info "Successfully connected sender to $(sender.host):$(sender.port)"
end


"""
    flush(sender::Sender)

Attempts to flush any unsent text to the server socket. In case of an error, an exception is thrown
"""
function flush(sender::Sender)
    ## Consume the remaining text
    try
        ## Flush the remaining bytes
        Base.write(sender.socket, sender.buffer)
        Base.flush(sender.socket)
        ## Clear the sender buffer
        clear(sender)
        @info "Flushed extra bytes to server..."
    catch err
        @error "Failed to write remaining bytes to server\n"
        throw(err)
    end

    return nothing
end


"""
    close(sender::Sender)

Attempts to close the sender's connection to the server. In case of an error, an exception is thrown
"""
function close(sender::Sender)
    try
        ## Flush the remaining data before closing
        QuestDBSender.flush(sender)
        ## CLose the socket
        Sockets.close(sender.socket)
    catch err
        throw(err)
    end

    @info "Successfully closed the connection to $(sender.host):$(sender.port)"
end

end

