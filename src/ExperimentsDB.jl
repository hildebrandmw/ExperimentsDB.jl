module ExperimentsDB

# stdlib
using Serialization

# deps
using DataStructures
using Tables
using PrettyTables

tokey(::Type{T}, x) where {K, T <: AbstractDict{K}} = K(x)

todict(::Type{T}, x) where {T} = T(tokey(T, k) => v for (k,v) in pairs(x))
todict(::Type{T}, x::T) where {T} = x
todict(x) = todict(SortedDict{Symbol,Any}, x)

# A row in the table
struct Entry <: Tables.AbstractRow
    parameters::SortedDict{Symbol,Any}
    data::Dict{String,Any}

    function Entry(parameters, data = Dict{String,Any}())
        parameters = todict(parameters)
        data = todict(Dict{String,Any}, data)
        return new(parameters, data)
    end
end

parameters(x::Entry) = getfield(x, :parameters)
Base.:(==)(x::Entry, y::Entry) = parameters(x) == parameters(y)
Base.in(x::SortedDict{Symbol,Any}, y::Entry) = todict(x) == parameters(y)

# Use a Sentinel instead of `nothing` because ... well ... the entries could BE `nothing`.
struct Sentinel end
function Base.issubset(x::SortedDict{Symbol,Any}, y::Entry)
    params = parameters(y)
    for (k,v) in x
        val = get(params, k, Sentinel())
        val == v || return false
    end
    return true
end
Base.keys(x::Entry) = keys(parameters(x))

Base.getindex(x::Entry) = getfield(x, :data)
Base.getindex(x::Entry, nm::Symbol) = parameters(x)[nm]

# Table API
Tables.getcolumn(x::Entry, nm::Symbol) = x[nm]
Tables.getcolumn(x::Entry, i::Int) = Tables.getcolumn(x, collect(keys(x))[i])
Tables.columnnames(x::Entry) = collect(keys(x))

# Adding empty fields to an Entry
function expand!(d::SortedDict{Symbol,Any}, ks)
    for k in ks
        get!(d, k, nothing)
    end
    return nothing
end
expand!(E::Entry, ks) = expand!(parameters(E), ks)

#####
##### Database
#####

struct Database
    entries::Vector{Entry}
    keys::Vector{Symbol}
end
Database() = Database(Entry[], Symbol[])
Base.keys(db::Database) = db.keys

Tables.istable(::Type{Database}) = true
Tables.rowaccess(::Type{Database}) = true
Tables.rows(db::Database) = db.entries

datamerge(a, b) = b
function Base.setindex!(db::Database, data, parameters)
    parameters = todict(parameters)
    data = todict(Dict{String,Any}, data)

    # Add in all of the keys from the db.
    expand!(parameters, keys(db))
    entry = Entry(parameters, data)

    # If the keys of this new entry are a subset of the keys already existing in the db,
    # then we have to check if this entry already exists in the Database.
    if issubset(keys(parameters), keys(db))
        i = findfirst(isequal(entry), Tables.rows(db))
        if !isnothing(i)
            merge!(datamerge, db[i][], data)
            return nothing
        end
    else
        # Expand the existing entries for any new keys that were potentially created.
        expand!.(Tables.rows(db), Ref(keys(entry)))
        append!(keys(db), setdiff(keys(entry), keys(db)))
        sort!(keys(db))
    end

    # Now we add a new row.
    push!(db.entries, entry)
    return nothing
end

# Indexing methods for searching and reducing the size of the table.
Base.length(x::Database) = length(x.entries)
Base.isempty(x::Database) = isempty(x.entries)
Base.iterate(x::Database) = iterate(x.entries)
Base.iterate(x::Database, s) = iterate(x.entries, s)
Base.eltype(x::Database) = eltyps(x.entries)

# Base.in
# We're essentially looking for `d` to have a subset of
Base.in(d::AbstractDict, s::Database) = any(x -> in(todict(d), x), s.entries)
Base.issubset(d::AbstractDict, s::Database) = any(x -> issubset(todict(d), x), s.entries)

# `getindex` Overloading
Base.getindex(x::Database, i::Integer) = x.entries[i]
Base.getindex(x::Database, i) = Database(x.entries[i], x.keys)
Base.getindex(x::Database, ::Nothing) = Database()
function Base.getindex(x::Database, i::SortedDict{Symbol,Any})
    return x[findall(x -> issubset(i, x), x.entries)]
end
Base.getindex(x::Database, i::NamedTuple) = x[todict(i)]
Base.getindex(x::Database; kw...) = x[(;kw...,)]
Base.getindex(x::Database, s::Symbol) = [ e[s] for e in x ]

function findonly(x::Database, i)
    inds = findall(x -> in(todict(i), x), x.entries)
    isempty(inds) && return nothing
    length(inds) > 1 && error("Found $inds matching entries!!")
    return x[first(inds)]
end

#####
##### Tables Interface
#####

# Pretty Printing
function Base.show(io::IO, db::Database)
    if length(db) == 0
        println("Empty Database")
    else
        PrettyTables.pretty_table(io, db, keys(db))
    end
end

#####
##### Saving and Loading
#####

load(path) = deserialize(path)::Database
function save(path, db)
    # Serialize to a temporary file and then use `mv`.
    # This is just in case we get interrupted while serializing.
    temp = tempname()
    serialize(temp, db)
    mv(temp, path; force = true)
    return nothing
end

end # module
