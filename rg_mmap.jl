using DataStructures
using HDF5
using EMIRT
using Interpolations

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

function reweight_affinity{Ta}(boundary::Dict{Tuple{Int32,Int32,Int32}, Ta}, boundary_cc::Set{Tuple{Int32,Int32,Int32}}, i::Int, aff_threshold::Float64)
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

function regiongraph{Ta,Ts}(aff::Array{Ta,4},seg::Array{Ts,3}, offset::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=offset
    (xend::Int32,yend::Int32,zend::Int32)=offset.+chunk_size

    real_x_boundary = false
    real_y_boundary = false
    if xend >= data_size[1]
        real_x_boundary = true
        xend = data_size[1]
    end
    if yend >= data_size[2]
        real_y_boundary = true
        yend = data_size[2]
    end

    println("$xstart, $ystart, $zstart")
    println("$xend, $yend, $zend")

    edges=Dict{Tuple{Ts,Ts},MeanEdge{Ta}}()
    idset = Set{UInt32}()
    maxid = zero(UInt32)
    aff_threshold = parse(Float64, ARGS[3])
    f1 = open("rg_volume_$(xstart)_$(ystart)_$(zstart).in","w")
    boundary_edges = Set{Tuple{Ts,Ts}}()
    for z=zstart:zend::Int32
      #println("processing z: $z")
      for y=ystart:yend::Int32
        for x=xstart:xend::Int32
          if seg[x,y,z]!=0   # ignore background voxels
            coord = (x::Int32,y::Int32,z::Int32)
            push!(idset,seg[x,y,z])
            if maxid < seg[x,y,z]
                maxid = seg[x,y,z]
            end
            if ( (x > xstart) && seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
              p = minmax(seg[x,y,z], seg[x-1,y,z])
              if y == ystart || z == zstart || y == yend || z == zend
                  push!(boundary_edges, p)
              end
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].area += 1
              edges[p].sum_affinity += aff[x,y,z,1]
              edges[p].boundaries[1][coord] = aff[x,y,z,1]
            end
            if ( (y > ystart) && seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
              p = minmax(seg[x,y,z], seg[x,y-1,z])
              if x == xstart || z == zstart || x == xend || z == zend
                  push!(boundary_edges, p)
              end
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].area += 1
              edges[p].sum_affinity += aff[x,y,z,2]
              edges[p].boundaries[2][coord] = aff[x,y,z,2]
            end
            if ( (z > zstart) && seg[x,y,z-1]!=0 && seg[x,y,z]!=seg[x,y,z-1])
              p = minmax(seg[x,y,z], seg[x,y,z-1])
              if x == xstart || y == ystart || x == xend || y == yend
                  push!(boundary_edges, p)
              end
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].area += 1
              edges[p].sum_affinity += aff[x,y,z,3]
              edges[p].boundaries[3][coord] = aff[x,y,z,3]
            end
          end
        end
      end
    end

    if !real_x_boundary
      x = xend+one(Int32)
      println("process 1 voxel overlapping yz face")
      for z=zstart:zend::Int32
        for y=ystart:yend::Int32
          if seg[x,y,z]==0   # ignore background voxels
              continue
          end
          coord = (x::Int32,y::Int32,z::Int32)
          if (seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
            p = minmax(seg[x,y,z], seg[x-1,y,z])
            if y == ystart || z == zstart || y == yend || z == zend
                push!(boundary_edges, p)
            end
            if !haskey(edges,p)
                edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
            end
            edges[p].area += 1
            edges[p].sum_affinity += aff[x,y,z,1]
            edges[p].boundaries[1][coord] = aff[x,y,z,1]
          end
        end
      end
    end

    if !real_y_boundary
      y = yend+one(Int32)
      println("process 1 voxel overlapping xz face")
      for z=zstart:zend::Int32
        for x=xstart:xend::Int32
          if seg[x,y,z]==0   # ignore background voxels
              continue
          end
          coord = (x::Int32,y::Int32,z::Int32)
          if (seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
            p = minmax(seg[x,y,z], seg[x,y-1,z])
            if x == xstart || z == zstart || x == xend || z == zend
                push!(boundary_edges, p)
            end
            if !haskey(edges,p)
                edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
            end
            edges[p].area += 1
            edges[p].sum_affinity += aff[x,y,z,2]
            edges[p].boundaries[2][coord] = aff[x,y,z,2]
          end
        end
      end
    end


    println("Calculating connect components")
    write(f1,"$maxid $(size(collect(idset))[1]+1) $(size(collect(edges))[1])\n")

    count_edges = 0
    for p in keys(edges)
        if p in boundary_edges
            open("$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).txt", "w") do f
                for i in 1:3
                    for k in keys(edges[p].boundaries[i])
                        write(f, "$i $(k[1]) $(k[2]) $(k[3]) $(edges[p].boundaries[i][k])\n")
                    end
                end
            end
            count_edges+=1
            continue
        end
        cc_sets = connect_component(union(Set(keys(edges[p].boundaries[1])),Set(keys(edges[p].boundaries[2])),Set(keys(edges[p].boundaries[3]))))
        cc_mean = Ta[]
        push!(cc_sets, union(Set(keys(edges[p].boundaries[1])),Set(keys(edges[p].boundaries[2])),Set(keys(edges[p].boundaries[3]))))
        for cc in cc_sets
            sum_area, sum_affinity = calculate_mean_affinity(edges[p].boundaries, cc)
            for i in 1:3
                w_affinity, w_area = reweight_affinity(edges[p].boundaries[i], cc, i, aff_threshold)
                sum_affinity += w_affinity
                sum_area += w_area
            end
            sum_affinity *= edges[p].area/sum_area
            if sum_area > 20
                push!(cc_mean, sum_affinity)
            end
        end
        if length(cc_mean) > 0
            write(f1,"$(p[1]) $(p[2]) $(Float64(edges[p].sum_affinity)) $(edges[p].area) $(p[1]) $(p[2]) $(maximum(cc_mean)) $(edges[p].area)\n")
        else
            write(f1,"$(p[1]) $(p[2]) $(Float64(edges[p].sum_affinity)) $(edges[p].area) $(p[1]) $(p[2]) $(Float64(edges[p].sum_affinity)) $(edges[p].area)\n")
        end
    end
    close(f1)
    println("boundary segments: $(length(boundary_edges)), edges: $(count_edges)")
    open("incomplete_edges_$(xstart)_$(ystart)_$(zstart).txt", "w") do f
        for p in boundary_edges
            write(f, "$(p[1]) $(p[2])\n")
        end
    end
end
f = h5open(ARGS[1])
#aff = f["affinityMap"]
aff = f["main"]
if ismmappable(aff)
    aff = readmmap(aff)
else
    aff = AffinityMap(read(aff))
end
close(f)

f = h5open(ARGS[2])
seg = f["main"]
if ismmappable(seg)
    seg = readmmap(seg)
else
    seg = Segmentation(read(seg))
end
close(f)

data_size = Int32[2048, 2048, 256]
chunk_size = Int32[1024, 1024, 128]
index = Int32[1,0,0]
offset = chunk_size.*index+1
println(offset)
@time regiongraph(aff,seg,offset)
