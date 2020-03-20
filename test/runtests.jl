using ExperimentsDB
using Test

using Tables
using DataStructures

# For testing `datamerge`
struct Merger
    val::Int
end
ExperimentsDB.datamerge(a::Merger, b::Merger) = Merger(a.val + b.val)

function keycheck(db::ExperimentsDB.Database)
    ks = keys(db)
    for entry in db
        @test collect(keys(entry)) == ks
    end
end

@testset "ExperimentsDB.jl" begin
    ### todict
    x = ExperimentsDB.todict((a = 1, b = 2))
    @test isa(x, DataStructures.SortedDict{Symbol, Any})
    @test length(x) == 2
    @test x[:a] == 1
    @test x[:b] == 2

    # Test pass-through logic
    @test ExperimentsDB.todict(x) === x

    # Pass in a normal dict
    y = Dict("a" => 1, "b" => 2)
    x = ExperimentsDB.todict(y)
    @test isa(x, DataStructures.SortedDict{Symbol, Any})
    @test length(x) == 2
    @test x[:a] == 1
    @test x[:b] == 2

    y = Dict(:a => 1, :b => 2)
    x = ExperimentsDB.todict(y)
    @test isa(x, DataStructures.SortedDict{Symbol, Any})
    @test length(x) == 2
    @test x[:a] == 1
    @test x[:b] == 2

    x = ExperimentsDB.todict(DataStructures.SortedDict{String,Any}, y)
    @test isa(x, DataStructures.SortedDict{String, Any})
    @test length(x) == 2
    @test x["a"] == 1
    @test x["b"] == 2

    ### Entry
    data = Dict(
        "entry_1" => 1,
        "entry_2" => 2,
    )
    A = ExperimentsDB.Entry((a = 1, b = 2), data)
    @test collect(keys(A)) == [:a, :b]
    @test A[] == data
    @test A[:a] == 1
    @test A[:b] == 2

    # Tables API
    @test Tables.columnnames(A) == [:a, :b]
    @test Tables.getcolumn(A, :a) == 1
    @test Tables.getcolumn(A,  1) == 1
    @test Tables.getcolumn(A, :b) == 2
    @test Tables.getcolumn(A,  2) == 2

    params = ExperimentsDB.parameters(A)
    @test length(params) == 2
    @test params[:a] == 1
    @test params[:b] == 2

    # Test ==
    B = ExperimentsDB.Entry(Dict("a" => 1, "b" => 2))
    @test A == B

    @test issubset(ExperimentsDB.todict((a = 1,)), A)
    @test issubset(ExperimentsDB.todict((b = 2,)), A)
    @test issubset(ExperimentsDB.todict((a = 1, b = 2,)), A)
    @test !issubset(ExperimentsDB.todict((a = 2, b = 2,)), A)
    @test !issubset(ExperimentsDB.todict((c = nothing,)), A)
    @test in(ExperimentsDB.todict((a = 1, b = 2,)), A)
    @test !in(ExperimentsDB.todict((a = 1,)), A)

    # expand!
    nt = (a = 1, b = 2)
    x = ExperimentsDB.todict(nt)
    ExperimentsDB.expand!(x, [:a, :c])
    @test x[:a] == 1
    @test x[:b] == 2
    @test x[:c] == nothing

    entry = ExperimentsDB.Entry(nt)
    ExperimentsDB.expand!(entry, [:a, :c])
    @test ExperimentsDB.parameters(entry) == x

    #####
    ##### DataBase
    #####
    db = ExperimentsDB.Database()
    @test length(db) == 0
    @test keys(db) == Symbol[]

    # Show empty database
    show(devnull, db)

    data1 = Dict(
        "merger" => Merger(10),
        "int" => 10,
    )
    data2 = Dict(
        "merger" => Merger(20),
        "int" => 20,
    )
    data3 = Dict(
        "merger" => Merger(30),
        "int" => 30
    )

    db[(a = 1, c = 2)] = data1
    @test length(db) == 1
    @test keys(db) == [:a, :c]

    ### We now have several different cases to test.
    ###
    ### Case 1: Keys of new entry are a subset of the existing keys, but a match is not found.
    ### Case 2: Keys of new entry are a subset of the existing keys, but a match is found.
    ### Case 3: Not a subset

    ## Case 1
    db[(a = 1,)] = data2
    @test length(db) == 2
    @test keys(db) == [:a, :c]

    # The second entry should have had its keys expanded
    entry = db[2]
    @test collect(keys(entry)) == [:a, :c]
    @test entry[:a] == 1
    @test entry[:c] == nothing
    @test db[1][] == data1
    @test db[2][] == data2

    ## Case 2
    db[(a = 1, c = 2)] = data2
    @test length(db) == 2
    keycheck(db)

    # Check if data was merged properly.
    data = db[1][]
    # The integer should have been replaced.
    @test data["int"] == 20
    # The Merger should have been merged
    @test data["merger"] == Merger(10 + 20)

    ## Case 3
    db[(b = 3,)] = data3
    @test length(db) == 3
    @test keys(db) == [:a, :b, :c]
    keycheck(db)

    ### Done testing merging logic
    # Test showing DB
    show(devnull, db)

    # Now, do some testing for "getindex"
    db2 = db[[1,2,]]
    @test isa(db2, ExperimentsDB.Database)
    @test length(db2) == 2
    @test db[1] == db2[1]
    @test db[1][] === db2[1][]
    @test db[2] == db2[2]
    @test db[2][] === db2[2][]

    # Test filtering logic
    db2 = db[a = 1]
    @test isa(db2, ExperimentsDB.Database)
    @test length(db2) == 2
    keycheck(db2)
    @test all(isequal(1), db2[:a])

    db2 = db[abba = nothing]
    @test isa(db2, ExperimentsDB.Database)
    @test length(db2) == 0

    # Finally, test the "in" and "subset" logic
    @test in(ExperimentsDB.todict((a = 1, b = nothing, c = 2)), db)
    @test !in(ExperimentsDB.todict((a = 1, c = 2)), db)
    @test issubset(ExperimentsDB.todict((a = 1, c = 2)), db)
    @test !issubset(ExperimentsDB.todict((a = 1, d = 2)), db)

    # Findonly
    @test isa(ExperimentsDB.findonly(db, (a = 1, b = nothing, c = 2)), ExperimentsDB.Entry)
    @test isnothing(ExperimentsDB.findonly(db, (a = 1, b = nothing)))
end

