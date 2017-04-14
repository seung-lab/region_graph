include("constants.jl")
include("mean_edge.jl")

function load_voxels(fn, edge)
    open(fn) do f
        for ln in eachline(f)
            data = split(ln, " ")
            i = parse(Int, data[1])
            x,y,z = [parse(Int32,x) for x in data[2:4]]
            aff = parse(Float64, data[5])
            coord = (x::Int32,y::Int32,z::Int32)
            edge.area += 1
            edge.sum_affinity += aff
            edge.boundaries[i][coord] = aff
        end
    end
end

function create_edges{Ts, Ta}(seg1::Ts, seg2::Ts, data, data_type::Ta)
    p = minmax(seg1, seg2)
    edge = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
    for s in data
        fn = "$(seg1)_$(seg2)_$(s).txt"
        load_voxels(fn,edge)
    end
    return process_edge(p, edge)
end

open(ARGS[1]) do fin
open(ARGS[2],"w") do fout
    for ln in eachline(fin)
        data = split(strip(ln), " ")
        seg1, seg2 = [parse(Int32, x) for x in data[1:2]]
        write(fout, create_edges(seg1, seg2, data[3:end], zero(Float32)))
    end
end
end
