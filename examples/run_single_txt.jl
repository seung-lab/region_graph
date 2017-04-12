include("../src/RegionGraph.jl")

using HDF5
using EMIRT
using RegionGraph

function write_txt{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,startIndex::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=startIndex

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
                        write(incomplete_edge_file, "$i $(k[1]) $(k[2]) $(k[3]) $(Float64(edge.boundaries[i][k]))\n")
                    end
                end
            end
            count_edges+=1
            continue
        end

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

    data_size = Int32[1024, 1024, 128]
    chunk_size = Int32[1024, 1024, 128]
    #chunk_size = Int32[2048, 2048, 256]
    for i in 0:1
        for j in 0:1
            index = Int32[i,j,0]
            startIndex = chunk_size.*index+1
            endIndex = startIndex.+chunk_size-1
            println(startIndex)

            real_x_boundary = false
            real_y_boundary = false
            if endIndex[1] >= data_size[1]
                real_x_boundary = true
                endIndex[1] = data_size[1]
            end
            if endIndex[2] >= data_size[2]
                real_y_boundary = true
                endIndex[2] = data_size[2]
            end

            @time edges, incomplete_segments = enumerate_edges(aff,seg,startIndex,endIndex,real_x_boundary,real_y_boundary)
            @time write_txt(edges,incomplete_segments,aff_threshold,startIndex)
        end
    end
end


main()
