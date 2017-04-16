include("SQLEdges.jl")
using SQLEdges
using RegionGraph
using HDF5
using BigArrays
using BigArrays.H5sBigArrays
using EMIRT

aff_in = H5sBigArray("../affinitymap/");
aff = AffinityMap(aff_in[29697:30720, 24577:25600, 1:128, 1:3])

seg_in = H5sBigArray("../segmentation/");
seg = Segmentation(seg_in[29697:30720, 24577:25600, 1:128])

aff_threshold = Float32(0.25)

data_size = Int32[1024, 1024, 128]
chunk_size = Int32[1024, 1024, 128]
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

        aff_view = aff[x:end_index[1],y:end_index[2],z:end_index[3],:]
        seg_view = seg[x:end_index[1],y:end_index[2],z:end_index[3]]

        @time edges, incomplete_segments = enumerate_edges(aff_view,seg_view)
        println("Writing SQL")
        #@time write_sql(edges,incomplete_segments,aff_threshold,global_offset)

        seg_pair = first(keys(edges))
        edge = edges[seg_pair]

        insert_incomplete_edge(edge, global_offset)

        read_edge = edge
        println(read_edge)


    end
end