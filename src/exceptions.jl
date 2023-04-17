module QuestDBExceptions

export IllegalColumnNameCharacterException, IllegalTableNameCharacterException, EmptyColumnNameException,
       ColumnNameLengthLimitExceededException, MultipleTableDefinitionsException, MissingTableDeclarationException,
       MalformedLineProtocolSyntaxException, UnsupportedAtColumnTypeException, UnsupportedColumnTypeException,
       QuestDBClientException


"""
    abstract type QuestDBClientException <: Exception end

Custom exception type used in QuestDBClient
"""
abstract type QuestDBClientException <: Exception end

"""
    struct IllegalTableNameCharacterException <: QuestDBClientException
        errorMessage::String
    end

Illegal Table Name character exception
"""
struct IllegalTableNameCharacterException <: QuestDBClientException
    errorMessage::String
end

"""
    struct IllegalColumnNameCharacterException <: QuestDBClientException
        errorMessage::String
    end

Illegal Column Name character exception
"""
struct IllegalColumnNameCharacterException <: QuestDBClientException
    errorMessage::String
end


"""
    struct EmptyColumnNameException <: QuestDBClientException
        errorMessage::String
    end

Empty Column Name exception
"""
struct EmptyColumnNameException <: QuestDBClientException
    errorMessage::String
end

"""
    struct ColumnNameLengthLimitExceededException <: QuestDBClientException
        message::String
    end

Column Name Length LimitnExceeded exception
"""
struct ColumnNameLengthLimitExceededException <: QuestDBClientException
    message::String
end


"""
    struct MultipleTableDefinitionsException <: QuestDBClientException
        message::String
    end

Multiple Table definitions detected
"""
struct MultipleTableDefinitionsException <: QuestDBClientException
    message::String
end

"""
    struct MissingTableDeclarationException <: QuestDBClientException
        message::String
    end

Missing table declaration detected -> May change this to MalformedLineProtocolSyntaxException
"""
struct MissingTableDeclarationException <: QuestDBClientException
    message::String
end

"""
    struct MalformedLineProtocolSyntaxException <: QuestDBClientException
        message::String
    end

Malformed Line Protocol syntax detected
"""
struct MalformedLineProtocolSyntaxException <: QuestDBClientException
    message::String
end

"""
    struct UnsupportedColumnTypeException <: QuestDBClientException
        message::String
    end

Unsupported Column Types detected
"""
struct UnsupportedColumnTypeException <: QuestDBClientException
    message::String
end

"""
    struct UnsupportedAtColumnTypeException <: QuestDBClientException
        message::String
    end

Specified At Column is not a timestamp
"""
struct UnsupportedAtColumnTypeException <: QuestDBClientException
    message::String
end
end