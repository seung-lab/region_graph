const data_start = Int32[1,1,1]
const data_end = Int32[2048,2048,256]
const data_size = data_end .- data_start .+ 1
const chunk_size = Int32[1024, 1024, 256]
const aff_threshold = Float32(0.25)
