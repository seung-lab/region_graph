include("../src/RegionGraph.jl")

using HDF5
using EMIRT
using RegionGraph

function write_txt{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,offset::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=offset

    println("Calculating connect components")
    complete_edge_file = open("rg_volume_$(xstart)_$(ystart)_$(zstart).in","w")

    count_edges = 0
    boundary_edges = Set{Tuple{Ts,Ts}}()
    for p in keys(edges)
        edge = edges[p]
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            open("$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).txt", "w") do incomplete_edge_file
                for i in 1:3
                    for k in keys(edge.boundaries[i])
                        write(incomplete_edge_file, "$i $(k[1]+xstart) $(k[2]+ystart) $(k[3]+zstart) $(Float64(edge.boundaries[i][k]))\n")
                    end
                end
            end
            count_edges+=1
            continue
        end
            
        total_boundaries = union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3])))
        area, sum_affinity = calculate_mean_affinity(edge.boundaries, total_boundaries)
        edge.sum_affinity = sum_affinity
        edge.area = area
        cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)

        if length(cc_means) > 0
            write(complete_edge_file, "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)\n")
        else
            write(complete_edge_file, "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)\n")
        end
    end
    close(complete_edge_file)

    println("boundary segments: $(length(boundary_edges)), edges: $(count_edges)")
    open("incomplete_edges_$(xstart)_$(ystart)_$(zstart).txt", "w") do f
        for p in boundary_edges
            write(f, "$(p[1]) $(p[2])\n")
        end
    end
end

function main()
    f = h5open(ARGS[1])
    #aff = f["affinityMap"]
    aff = f["img"]
    if ismmappable(aff)
        aff = readmmap(aff)
    else
        aff = AffinityMap(read(aff))
    end
    close(f)

    f = h5open(ARGS[2])
    seg = f["img"]
    if ismmappable(seg)
        seg = readmmap(seg)
    else
        seg = Segmentation(read(seg))
    end
    close(f)

    aff_threshold = parse(Float32, ARGS[3])

    println("begin run");
    data_size = Int32[768, 768, 128]
    chunk_size = Int32[256, 256, 128]
    #chunk_size = Int32[2048, 2048, 256]
    dilation_size = Int32[1,1,0]
    z = 1
    for x in 1:chunk_size[1]:data_size[1]
        for y in 1:chunk_size[2]:data_size[2]
            global_offset = Int32[x, y, 1].-1 # -1 because julia
            end_index = global_offset.+chunk_size.+dilation_size

            if end_index[1] >= data_size[1]
                end_index[1] = data_size[1]
            end
            if end_index[2] >= data_size[2]
                end_index[2] = data_size[2]
            end

            println(global_offset)
            println(end_index)

            aff_view = aff[x:end_index[1],y:end_index[2],z:end_index[3],:]
            seg_view = seg[x:end_index[1],y:end_index[2],z:end_index[3]]

            @time edges, incomplete_segments = enumerate_edges(aff_view,seg_view)
            @time write_txt(edges,incomplete_segments,aff_threshold,global_offset)

        end
    end
end


main()
