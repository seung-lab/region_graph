#const data_start = Int32[1,1,1]
#const data_end = Int32[2048,2048,256]
const data_start = Int32[28673, 24065, 2]
const data_end = Int32[80634, 60859, 996]
#const data_end = data_start .+ Int32[3999,3999,995] - one(Int32)
const chunk_size = Int32[1024, 1024, 995]
const aff_threshold = Float32(0.25)
