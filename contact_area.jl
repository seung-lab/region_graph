include("constants.jl")
using DataStructures

typealias SemanticInfo Array{Array{Float32,1},1}

abstract Edge
type ContactEdge{Ta} <: Edge
    boundaries::Array{Dict{Array{Int32,1}, SemanticInfo},1}
end

function connect_component(boundary::Set{Array{Int32,1}})
    cc_sets = Set{Array{Int32,1}}[]
    visited = Set{Array{Int32,1}}()
    offsets = [[i,j,k] for i in -1:1 for j in -1:1 for k in -1:1]
    deleteat!(offsets, 14)
    for root in boundary
        if root in visited
            continue
        end
        cc = Set{Array{Int32,1}}()
        queue = Queue(Array{Int32,1})
        enqueue!(queue, root)

        while length(queue) > 0
            root = dequeue!(queue)
            if root in visited
                continue
            end

            push!(visited,root)
            push!(cc, root)
            for offset in offsets
                neighbour = root .+ offset
                if !(neighbour in visited) && neighbour in boundary
                    enqueue!(queue, neighbour)
                end
            end
        end
        push!(cc_sets,cc)
    end
    return cc_sets
end

function count_edges(boundaries::Array{Dict{Array{Int32,1}, SemanticInfo},1}, boundary_cc::Set{Array{Int32,1}})
    sem_sum_1 = zeros(Float32,5)
    sem_sum_2 = zeros(Float32,5)
    counts = Int64[]
    bbox = Int32[chunk_size[1], chunk_size[2], chunk_size[3],0,0,0]
    com = Int[0,0,0]
    vol = 0
    for v in boundary_cc
        com[1] += v[1]
        com[2] += v[2]
        com[3] += v[3]
        vol += 1
        bbox[1] = min(bbox[1],v[1])
        bbox[2] = min(bbox[2],v[2])
        bbox[3] = min(bbox[3],v[3])
        bbox[4] = max(bbox[4],v[1])
        bbox[5] = max(bbox[5],v[2])
        bbox[6] = max(bbox[6],v[3])
    end

    for i in 1:3
        count = 0
        for v in intersect(keys(boundaries[i]),boundary_cc)
            count += 1
            #println(boundaries[i][v])
            sem_sum_1 += boundaries[i][v][1]
            sem_sum_2 += boundaries[i][v][2]
        end
        append!(counts, count)
    end
    return counts, vol, com, bbox, sem_sum_1, sem_sum_2
end


function process_edge!(p, edge, results)
    cc_sets = connect_component(union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3]))))
    for i in 1:length(cc_sets)
        counts, vol, com, bbox, sem_sum_1, sem_sum_2 = count_edges(edge.boundaries, cc_sets[i])
        push!(results,[counts, vol, com, bbox, sem_sum_1, sem_sum_2])
    end
end

