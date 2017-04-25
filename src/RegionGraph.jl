module RegionGraph

using DataStructures

export Edge, MeanEdge, enumerate_edges, calculate_mean_affinity, calculate_mean_affinity_pluses, reweight_affinity, connect_component

__precompile__()

abstract Edge
type MeanEdge{Ts,Ta} <: Edge
    seg_id_1::Ts
    seg_id_2::Ts
    area::Float32
    sum_affinity::Ta
    boundaries::Array{Dict{Tuple{Int32,Int32,Int32}, Ta},1}
end

function enumerate_edges{Ta,Ts}(aff::Array{Ta,4},seg::Array{Ts,3})
    (xstart::Int32,ystart::Int32,zstart::Int32)=(1,1,1)
    (xend::Int32,yend::Int32,zend::Int32,_)=size(aff)
    
    edges=Dict{Tuple{Ts,Ts},MeanEdge{Ts,Ta}}()
    boundary_edges = Set{Tuple{Ts,Ts}}()
    incomplete_segments = Set{Ts}()
    for z=zstart:zend::Int32
      #println("processing z: $z")
      for y=ystart:yend::Int32
        for x=xstart:xend::Int32
          if seg[x,y,z]!=0   # ignore background voxels
            isIncomplete = x == xstart || y == ystart || x == xend || y == yend
            if isIncomplete
                push!(incomplete_segments, seg[x,y,z])
            end
            coord = (x::Int32,y::Int32,z::Int32)
            if ( (x > xstart) && seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
              p = minmax(seg[x,y,z], seg[x-1,y,z])
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ts,Ta}(p[1], p[2], zero(Float32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].boundaries[1][coord] = aff[x,y,z,1]
              if !isIncomplete
                edges[p].area += 1
                edges[p].sum_affinity += aff[x,y,z,1]
              end
            end
            if ( (y > ystart) && seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
              p = minmax(seg[x,y,z], seg[x,y-1,z])
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ts,Ta}(p[1], p[2], zero(Float32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].boundaries[2][coord] = aff[x,y,z,2]
              if !isIncomplete
                edges[p].area += 1
                edges[p].sum_affinity += aff[x,y,z,2]
              end
            end
            if ( (z > zstart) && seg[x,y,z-1]!=0 && seg[x,y,z]!=seg[x,y,z-1])
              p = minmax(seg[x,y,z], seg[x,y,z-1])
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ts,Ta}(p[1], p[2], zero(Float32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].boundaries[3][coord] = aff[x,y,z,3]
              if !isIncomplete
                edges[p].area += 1
                edges[p].sum_affinity += aff[x,y,z,3]
              end
            end
          end
        end
      end
    end
    return edges, incomplete_segments
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

function reweight_affinity{Ta}(boundary::Dict{Tuple{Int32,Int32,Int32}, Ta}, boundary_cc::Set{Tuple{Int32,Int32,Int32}}, i::Int, aff_threshold::Ta)
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

function calculate_mean_affinity_pluses{Ta, Ts}(p::Tuple{Ts, Ts}, edge::MeanEdge{Ts,Ta}, aff_threshold::Ta)
    cc_sets = connect_component(union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3]))))
    cc_means = Float32[]
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
            push!(cc_means, sum_affinity)
        end
    end
    return cc_means
end

end # module RegionGraph

