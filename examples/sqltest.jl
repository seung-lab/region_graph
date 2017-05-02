include("../src/SQLEdges.jl")
using SQLEdges
using RegionGraph
using HDF5
using BigArrays
using BigArrays.H5sBigArrays
using EMIRT

aff_in = H5sBigArray("../affinitymap/");
aff = AffinityMap(aff_in[29697:30720, 24577:25600, 1:1024, 1:3])

seg_in = H5sBigArray("../segmentation/");
seg = Segmentation(seg_in[29697:30720, 24577:25600, 1:1024])

aff_threshold = Float32(0.25)

data_size = Int32[1024, 1024, 1024]
chunk_size = Int32[1024, 1024, 1024]
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

        seg_pairs = [keys(edges)...]

        #edge = edges[seg_pairs[1]]
        #println(edge)

        #insert_incomplete_edge(edge, global_offset)
        insert_incomplete_edges([values(edges)...], global_offset)

        #read_edge = get_incomplete_edge((4320, 959519), aff_threshold)
        #println(read_edge)

        #read_edges = get_incomplete_edges(seg_pairs, aff_threshold)
        #println(read_edges[seg_pairs[10]])


    end
end