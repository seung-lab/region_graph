using DataStructures

abstract Edge
type MeanEdge{Ta} <: Edge
    area::Float64
    sum_affinity::Ta
    boundaries::Array{Dict{Tuple{Int32,Int32,Int32}, Ta},1}
end

function connect_component(boundary::Set{Tuple{Int32,Int32,Int32}})
    cc_sets = Set{Tuple{Int32,Int32,Int32}}[]
    visited = Set{Tuple{Int32,Int32,Int32}}()
    for root in boundary
        if root in visited
            continue
        end
        cc = Set{Tuple{Int32,Int32,Int32}}()
        queue = Queue(Tuple{Int32,Int32,Int32})
        enqueue!(queue, root)

        while length(queue) > 0
            root = dequeue!(queue)
            if root in visited
                continue
            end

            push!(visited,root)
            push!(cc, root)
            neighbours = [(root[1]-one(Int32), root[2], root[3]),
                          (root[1]+one(Int32), root[2], root[3]),
                          (root[1], root[2]-one(Int32), root[3]),
                          (root[1], root[2]+one(Int32), root[3]),
                          (root[1], root[2], root[3]-one(Int32)),
                          (root[1], root[2], root[3]+one(Int32))]
            for neighbour in neighbours
                if !(neighbour in visited) && neighbour in boundary
                    enqueue!(queue, neighbour)
                end
            end
        end
        push!(cc_sets,cc)
    end
    return cc_sets
end

function calculate_mean_affinity{Ta}(boundaries::Array{Dict{Tuple{Int32,Int32,Int32}, Ta},1}, boundary_cc::Set{Tuple{Int32,Int32,Int32}})
    sum = 0
    num = zero(Ta)
    for b in boundaries
        for v in intersect(keys(b),boundary_cc)
            sum += 1
            num += b[v]
        end
    end
    return sum, num
end

function reweight_affinity{Ta}(boundary::Dict{Tuple{Int32,Int32,Int32}, Ta}, boundary_cc::Set{Tuple{Int32,Int32,Int32}}, i::Int, aff_threshold::Float32)
    weighted_aff = 0
    weighted_area = 0
    visited = Set{Tuple{Int32,Int32,Int32}}()
    for root in intersect(keys(boundary),boundary_cc)
        if root in visited || boundary[root] < aff_threshold
            continue
        end
        cc_aff = zero(Ta)
        cc_area = 0
        queue = Queue(Tuple{Int32,Int32,Int32})
        enqueue!(queue, root)

        while length(queue) > 0
            root = dequeue!(queue)
            if root in visited
                continue
            end

            push!(visited,root)
            cc_aff += boundary[root]
            cc_area += 1
            #neighbours = generate_neighbours(root, i)
            neighbours = [(root[1]-one(Int32), root[2], root[3]),
                          (root[1]+one(Int32), root[2], root[3]),
                          (root[1], root[2]-one(Int32), root[3]),
                          (root[1], root[2]+one(Int32), root[3]),
                          (root[1], root[2], root[3]-one(Int32)),
                          (root[1], root[2], root[3]+one(Int32))]
            for neighbour in neighbours
                if !(neighbour in visited) && neighbour in keys(boundary)
                    if boundary[neighbour] > aff_threshold
                        enqueue!(queue, neighbour)
                    end
                end
            end
        end
        if cc_aff > 1
            weighted_aff += cc_aff*(cc_aff - 1)
            weighted_area += (cc_aff - 1)*cc_area
        end
    end
    return weighted_aff, weighted_area
end

function process_edge!(p, edge, results)
    cc_sets = connect_component(union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3]))))
    cc_mean = Float32[]
    push!(cc_sets, union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3]))))
    for cc in cc_sets
        sum_area, sum_affinity = calculate_mean_affinity(edge.boundaries, cc)
        for i in 1:3
            w_affinity, w_area = reweight_affinity(edge.boundaries[i], cc, i, aff_threshold)
            sum_affinity += w_affinity
            sum_area += w_area
        end
        sum_affinity *= edge.area/sum_area
        if sum_area > 20
            push!(cc_mean, sum_affinity)
        end
    end
    results[1] = Float64(edge.sum_affinity)
    results[2] = results[4] = Float64(edge.area)
    if length(cc_mean) > 0
        results[3] = Float64(maximum(cc_mean))
    else
        results[3] = results[1]
    end
end

