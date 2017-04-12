using HDF5
include("expand_segments.jl")

function get_aff(fn)
    f = h5open(fn)
    aff = f["main"]
    if ismmappable(aff)
        aff = readmmap(aff)
    else
        aff = AffinityMap(read(aff))
    end
    close(f)
    return aff
end

function get_seg(fn)
    f = h5open(fn)
    seg = f["main"]
    if ismmappable(seg)
        seg = readmmap(seg)
    else
        seg = Segmentation(read(seg))
    end
    close(f)
    return seg
end

include("constants.jl")
index = [parse(Int32, x) for x in ARGS[3:end]]
offset = data_start.+chunk_size.*index
margin = [0,0,0]

aff_start=[max(data_start[i], offset[i]) for i in 1:3]
aff_end=[min(data_end[i], offset[i]+chunk_size[i]) for i in 1:3]
seg_start=[max(data_start[i], offset[i]-margin[i]) for i in 1:3]
seg_end=[min(data_end[i], offset[i]+chunk_size[i]+margin[i]) for i in 1:3]

println("aff_start: $aff_start")
println("aff_end: $aff_end")
println("seg_start: $seg_start")
println("seg_end: $seg_end")

aff = get_aff(ARGS[1])[aff_start[1]:aff_end[1], aff_start[2]:aff_end[2], aff_start[3]:aff_end[3], 1:3]
seg_extra = get_seg(ARGS[2])[seg_start[1]:seg_end[1], seg_start[2]:seg_end[2], seg_start[3]:seg_end[3]]

cut_before = min(margin, seg_start - data_start)
cut_after = min(margin, data_end - seg_end)

println("cut_before: $cut_before")
println("cut_after: $cut_after")

h5write("aff_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "main", aff)
h5write("seg_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "main", seg_extra[1+cut_before[1]:end-cut_after[1], 1+cut_before[2]:end-cut_after[2],1+cut_before[3]:end-cut_after[3]])

new_seg = expand_segments(seg_extra)
h5write("seg_exp_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "main", new_seg[1+cut_before[1]:end-cut_after[1], 1+cut_before[2]:end-cut_after[2],1+cut_before[3]:end-cut_after[3]])

