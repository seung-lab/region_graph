using S3Dicts
using BigArrays
using HDF5
include("expand_segments.jl")

function get_aff()
    da = S3Dict("s3://neuroglancer/pinky40_v3/affinitymap-jnet/4_4_40/")
    aff = BigArray(da)
    return aff
end

function get_seg()
    ds = S3Dict("s3://neuroglancer/pinky40_v3/watershed/4_4_40/")
    seg = BigArray(ds)
    return seg
end

include("constants.jl")
index = [parse(Int32, x) for x in ARGS[2:end]]
offset = data_start.+chunk_size.*index
margin = [500,500,0]

aff_start=[max(data_start[i], offset[i]) for i in 1:3]
aff_end=[min(data_end[i], offset[i]+chunk_size[i]) for i in 1:3]
seg_start=[max(data_start[i], offset[i]-margin[i]) for i in 1:3]
seg_end=[min(data_end[i], offset[i]+chunk_size[i]+margin[i]) for i in 1:3]

println("aff_start: $aff_start")
println("aff_end: $aff_end")
println("seg_start: $seg_start")
println("seg_end: $seg_end")

cut_before = min(margin, aff_start - data_start)
cut_after = min(margin, data_end - aff_end)

println("cut_before: $cut_before")
println("cut_after: $cut_after")

if ARGS[1] == "aff"
    aff = get_aff()[aff_start[1]:aff_end[1], aff_start[2]:aff_end[2], aff_start[3]:aff_end[3], 1:3]
    f = h5open("aff_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "w")
    f["main", "chunk", (256,256,64,3), "blosc", 3] = aff
    close(f)
end

if ARGS[1] == "seg"
    seg_extra = get_seg()[seg_start[1]:seg_end[1], seg_start[2]:seg_end[2], seg_start[3]:seg_end[3]]
    f = h5open("seg_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "w")
    f["main", "chunk", (256,256,64), "blosc", 3] = seg_extra[1+cut_before[1]:end-cut_after[1], 1+cut_before[2]:end-cut_after[2],1+cut_before[3]:end-cut_after[3]]
    close(f)

    new_seg = expand_segments(seg_extra)

    f = h5open("seg_exp_chunk_$(index[1])_$(index[2])_$(index[3]).h5", "w")
    f["main", "chunk", (256,256,64), "blosc", 3] = new_seg[1+cut_before[1]:end-cut_after[1], 1+cut_before[2]:end-cut_after[2],1+cut_before[3]:end-cut_after[3]]
    close(f)
end

