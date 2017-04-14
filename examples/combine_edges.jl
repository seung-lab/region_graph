include("../src/RegionGraph.jl")
using RegionGraph

function load_voxels(fn, edge)
    open(fn) do f
        for ln in eachline(f)
            data = split(ln, " ")
            i = parse(Int, data[1])
            x,y,z = [parse(Int32,x) for x in data[2:4]]
            aff = parse(Float32, data[5])
            coord = (x::Int32,y::Int32,z::Int32)
            edge.boundaries[i][coord] = aff
        end
    end
end

function create_edges{Ts, Ta}(seg1::Ts, seg2::Ts, aff_threshold::Ta)
    p = minmax(seg1, seg2)
    edge = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
    re = Regex("^$(seg1)_$(seg2)_\\d+_\\d+_\\d+.txt")
    for fn in filter(x->ismatch(re,x), readdir("."))
        load_voxels(fn,edge)
    end

    total_boundaries = union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3])))
    area, sum_affinity = calculate_mean_affinity(edge.boundaries, total_boundaries)
    edge.sum_affinity = sum_affinity
    edge.area = area

    cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)

    if length(cc_means) > 0
        return "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)\n"
    else
        return "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)\n"
    end

end

aff_threshold = parse(Float32, ARGS[1])

open(ARGS[2]) do fin
open(ARGS[3],"w") do fout
    for ln in eachline(fin)
        seg1, seg2 = [parse(Int64, x) for x in split(ln, " ")]
        write(fout, create_edges(seg1, seg2, aff_threshold))
    end
end
end
