include("mean_edge.jl")

function load_voxels(fn, edge)
    open(fn) do f
        for ln in eachline(f)
            data = split(ln, " ")
            i = parse(Int, data[1])
            x,y,z = [parse(Int32,x) for x in data[2:4]]
            aff = parse(Float32, data[5])
            coord = (x::Int32,y::Int32,z::Int32)
            edge.area += 1
            edge.sum_affinity += aff
            edge.boundaries[i][coord] = aff
        end
    end
end

function create_edges{Ts, Ta}(seg1::Ts, seg2::Ts, data_type::Ta)
    p = minmax(seg1, seg2)
    edges=Dict{Tuple{Ts,Ts},MeanEdge{Float32}}()
    edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
    re = Regex("^$(seg1)_$(seg2)_\\d+_\\d+_\\d+.txt")
    for fn in filter(x->ismatch(re,x), readdir("."))
        load_voxels(fn,edges[p])
    end
    open("edges/$(seg1)_$(seg2)_entry.txt", "w") do f
        write(f, process_edge(p, edges))
    end
end

aff_threshold = parse(Float64, ARGS[1])
seg1 = parse(Int64, ARGS[2])
seg2 = parse(Int64, ARGS[3])
create_edges(seg1, seg2, zero(Float32))
