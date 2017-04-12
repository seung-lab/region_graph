using HDF5
using EMIRT
using Interpolations

include("mean_edge.jl")

function regiongraph{Ta,Ts}(aff::Array{Ta,4},seg::Array{Ts,3}, offset::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=offset
    (xend::Int32,yend::Int32,zend::Int32)=offset.+chunk_size-1

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
    f1 = open("rg_volume_$(xstart)_$(ystart)_$(zstart).in","w")
    boundary_edges = Set{Tuple{Ts,Ts}}()
    incomplete_segments = Set{Ts}()
    for z=zstart:zend::Int32
      #println("processing z: $z")
      for y=ystart:yend::Int32
        for x=xstart:xend::Int32
          if seg[x,y,z]!=0   # ignore background voxels
            if x == xstart || y == ystart || x == xend || y == yend
                push!(incomplete_segments, seg[x,y,z])
            end
            coord = (x::Int32,y::Int32,z::Int32)
            push!(idset,seg[x,y,z])
            if maxid < seg[x,y,z]
                maxid = seg[x,y,z]
            end
            if ( (x > xstart) && seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
              p = minmax(seg[x,y,z], seg[x-1,y,z])
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].area += 1
              edges[p].sum_affinity += aff[x,y,z,1]
              edges[p].boundaries[1][coord] = aff[x,y,z,1]
            end
            if ( (y > ystart) && seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
              p = minmax(seg[x,y,z], seg[x,y-1,z])
              if !haskey(edges,p)
                  edges[p] = MeanEdge{Ta}(zero(UInt32),zero(Ta),Dict{Tuple{Int32,Int32,Int32}, Ta}[Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}(),Dict{Tuple{Int32,Int32,Int32}, Ta}()])
              end
              edges[p].area += 1
              edges[p].sum_affinity += aff[x,y,z,2]
              edges[p].boundaries[2][coord] = aff[x,y,z,2]
            end
            if ( (z > zstart) && seg[x,y,z-1]!=0 && seg[x,y,z]!=seg[x,y,z-1])
              p = minmax(seg[x,y,z], seg[x,y,z-1])
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
          push!(incomplete_segments, seg[x,y,z])
          coord = (x::Int32,y::Int32,z::Int32)
          if (seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
            p = minmax(seg[x,y,z], seg[x-1,y,z])
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
          push!(incomplete_segments, seg[x,y,z])
          coord = (x::Int32,y::Int32,z::Int32)
          if (seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
            p = minmax(seg[x,y,z], seg[x,y-1,z])
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
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            open("$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).txt", "w") do f
                for i in 1:3
                    for k in keys(edges[p].boundaries[i])
                        write(f, "$i $(k[1]) $(k[2]) $(k[3]) $(Float64(edges[p].boundaries[i][k]))\n")
                    end
                end
            end
            count_edges+=1
            continue
        end
        write(f1, process_edge(p,edges[p]))

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

aff_threshold = parse(Float64, ARGS[3])

data_size = Int32[2048, 2048, 256]
chunk_size = Int32[1024, 1024, 128]
index = Int32[1,0,0]
offset = chunk_size.*index+1
println(offset)
@time regiongraph(aff,seg,offset)
