using Base.Threads
using HDF5

include("contact_area.jl")

function regiongraph{Ta,Ts}(sem::Array{Ta,4},seg::Array{Ts,3}, offset::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=Int32[1,1,1]
    #(xend::Int32,yend::Int32,zend::Int32)=offset.+chunk_size-1
    (xend::Int32,yend::Int32,zend::Int32)=collect(size(seg)).-Int32[1,1,0]

    real_x_boundary = false
    real_y_boundary = false
    if offset[1]+chunk_size[1]-1 >= data_end[1]
        real_x_boundary = true
        xend += 1
    end
    if offset[2]+chunk_size[2]-1 >= data_end[2]
        real_y_boundary = true
        yend += 1
    end

    println("$xstart, $ystart, $zstart")
    println("$xend, $yend, $zend")

    edges=Dict{Tuple{Ts,Ts},ContactEdgeBool}()
    edges_array = Array{Tuple{Ts,Ts},1}()
    f1 = open("rg_volume_$(ARGS[2])_$(index[1])_$(index[2])_$(index[3]).in","w")
    boundary_edges = Set{Tuple{Ts,Ts}}()
    incomplete_segments = Set{Ts}()
    coord = Int32[1,1,1]
    for z=zstart:zend::Int32
      coord[3] = z
      for y=ystart:yend::Int32
        coord[2] = y
        for x=xstart:xend::Int32
          if seg[x,y,z]!=0   # ignore background voxels
            if x == xstart || y == ystart || x == xend || y == yend
                push!(incomplete_segments, seg[x,y,z])
            end
            coord[1] = x
            if ( (x > xstart) && seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
              p = minmax(seg[x,y,z], seg[x-1,y,z])
              if !haskey(edges,p)
                  edges[p] = ContactEdgeBool(Dict{Array{Int32,1}, Bool}[Dict{Array{Int32,1}, Bool}(),Dict{Array{Int32,1}, Bool}(), Dict{Array{Int32,1}, Bool}()])
              end
              if p[1] == seg[x,y,z]
                  edges[p].boundaries[1][deepcopy(coord)] = true
              else
                  edges[p].boundaries[1][deepcopy(coord)] = false
              end
            end
            if ( (y > ystart) && seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
              p = minmax(seg[x,y,z], seg[x,y-1,z])
              if !haskey(edges,p)
                  edges[p] = ContactEdgeBool(Dict{Array{Int32,1}, Bool}[Dict{Array{Int32,1}, Bool}(),Dict{Array{Int32,1}, Bool}(), Dict{Array{Int32,1}, Bool}()])
              end
              if p[1] == seg[x,y,z]
                  edges[p].boundaries[2][deepcopy(coord)] = true
              else
                  edges[p].boundaries[2][deepcopy(coord)] = false
              end
            end
            if ( (z > zstart) && seg[x,y,z-1]!=0 && seg[x,y,z]!=seg[x,y,z-1])
              p = minmax(seg[x,y,z], seg[x,y,z-1])
              if !haskey(edges,p)
                  edges[p] = ContactEdgeBool(Dict{Array{Int32,1}, Bool}[Dict{Array{Int32,1}, Bool}(),Dict{Array{Int32,1}, Bool}(), Dict{Array{Int32,1}, Bool}()])
              end
              if p[1] == seg[x,y,z]
                  edges[p].boundaries[3][deepcopy(coord)] = true
              else
                  edges[p].boundaries[3][deepcopy(coord)] = false
              end
            end
          end
        end
      end
    end

    if !real_x_boundary
      x = xend+one(Int32)
      coord[1] = x
      println("process 1 voxel overlapping yz face")
      for z=zstart:zend::Int32
        coord[3] = z
        for y=ystart:yend::Int32
          if seg[x,y,z]==0   # ignore background voxels
              continue
          end
          push!(incomplete_segments, seg[x,y,z])
          coord[2] = y
          if (seg[x-1,y,z]!=0 && seg[x,y,z]!=seg[x-1,y,z])
            p = minmax(seg[x,y,z], seg[x-1,y,z])
            if !haskey(edges,p)
                edges[p] = ContactEdgeBool(Dict{Array{Int32,1}, Bool}[Dict{Array{Int32,1}, Bool}(),Dict{Array{Int32,1}, Bool}(), Dict{Array{Int32,1}, Bool}()])
            end
            if p[1] == seg[x,y,z]
                edges[p].boundaries[1][deepcopy(coord)] = true
            else
                edges[p].boundaries[1][deepcopy(coord)] = false
            end
          end
        end
      end
    end

    if !real_y_boundary
      y = yend+one(Int32)
      coord[2] = y
      println("process 1 voxel overlapping xz face")
      for z=zstart:zend::Int32
        coord[3] = z
        for x=xstart:xend::Int32
          if seg[x,y,z]==0   # ignore background voxels
              continue
          end
          coord[1] = x
          push!(incomplete_segments, seg[x,y,z])
          if (seg[x,y-1,z]!=0 && seg[x,y,z]!=seg[x,y-1,z])
            p = minmax(seg[x,y,z], seg[x,y-1,z])
            if !haskey(edges,p)
                edges[p] = ContactEdgeBool(Dict{Array{Int32,1}, Bool}[Dict{Array{Int32,1}, Bool}(),Dict{Array{Int32,1}, Bool}(), Dict{Array{Int32,1}, Bool}()])
            end
            if p[1] == seg[x,y,z]
                edges[p].boundaries[2][deepcopy(coord)] = true
            else
                edges[p].boundaries[2][deepcopy(coord)] = false
            end
          end
        end
      end
    end

    println("Calculating connect components")

    count_edges = 0
    f_incomp = open("$(ARGS[2])/incomplete_edges_$(index[1])_$(index[2])_$(index[3]).txt", "w")
    f_comp = open("$(ARGS[1])/complete_edges_$(index[1])_$(index[2])_$(index[3]).txt", "w")
    for p in keys(edges)
        fn = "$(ARGS[1])/$(p[1])_$(p[2])_$(index[1])_$(index[2])_$(index[3]).txt"
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            fn = "$(ARGS[2])/$(p[1])_$(p[2])_$(index[1])_$(index[2])_$(index[3]).txt"
            write(f_incomp, "$(p[1]) $(p[2])\n")
        else
            write(f_comp, "$(p[1]) $(p[2])\n")
        end
        open(fn, "w") do f
            for i in 1:3
                for k in keys(edges[p].boundaries[i])
                    write(f, i)
                    write(f, (k+offset-one(Int32)))
                    write(f, edges[p].boundaries[i][k])
                    #if i == 1
                    #    if edges[p].boundaries[i][k]
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #        write(f, sem[k[1]-1,k[2],k[3],:])
                    #    else
                    #        write(f, sem[k[1]-1,k[2],k[3],:])
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #    end
                    #elseif i == 2
                    #    if edges[p].boundaries[i][k]
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #        write(f, sem[k[1],k[2]-1,k[3],:])
                    #    else
                    #        write(f, sem[k[1],k[2]-1,k[3],:])
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #    end
                    #elseif i == 3
                    #    if edges[p].boundaries[i][k]
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #        write(f, sem[k[1],k[2],k[3]-1,:])
                    #    else
                    #        write(f, sem[k[1],k[2],k[3]-1,:])
                    #        write(f, sem[k[1],k[2],k[3],:])
                    #    end
                    #end
                end
            end
        end
    end
    #for i in 1:length(edges_array)
    #    write(f1, "$(edges_array[i][1]) $(edges_array[i][2]) $(complete_edge_output[i][1]) $(complete_edge_output[i][2]) $(edges_array[i][1]) $(edges_array[i][2]) $(complete_edge_output[i][3]) $(complete_edge_output[i][4])\n")
    #end
    close(f1)
    close(f_incomp)
    close(f_comp)
end

index = [parse(Int32, x) for x in ARGS[3:end]]

f = h5open("$(ARGS[1])_$(index[1])_$(index[2])_$(index[3]).h5")
#aff = f["affinityMap"]
aff = f["main"]
if ismmappable(aff)
    aff = readmmap(aff)
else
    aff = read(aff)
end
close(f)

f = h5open("$(ARGS[2])_$(index[1])_$(index[2])_$(index[3]).h5")
seg = f["main"]
if ismmappable(seg)
    seg = readmmap(seg)
else
    seg = read(seg)
end
close(f)

offset = data_start.+chunk_size.*index
println(offset)
@time regiongraph(aff,seg,offset)
