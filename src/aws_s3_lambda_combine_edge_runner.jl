push!(LOAD_PATH, ".")

using AWSCore
using AWSS3
using AWSLambda
using RegionGraph
using RegionGraphS3Lambda
aws = AWSCore.aws_config()

# get the keys ahead of time so we don't have to pay for additional query
# overhead
create_edge_lambda = @lambda aws function create_edge_jl(seg_1::Int64,
                                                      seg_2::Int64,
                                                      bucket::String,
                                                      s3_keys::Array{String, 1},
                                                      aff_threshold::Float32)
    #@assert !Base.stale_cachefile("/var/task/julia/RegionGraph/RegionGraph.jl",
                                  #Base.LOAD_CACHE_PATH[1] * "/RegionGraph.ji")

    using AWSS3
    using AWSCore
    using RegionGraph
    using RegionGraphS3Lambda

    return create_edge(seg_1, seg_2, "seunglab-test", s3_keys, aff_threshold)
end

lo = s3_list_objects(aws, "seunglab-test", "region_graph/edges/")
pairs = Dict{Tuple{Int64, Int64}, Array{String, 1}}()

for o in lo
   m = match(r"(\d+)_(\d+)_\d+_\d+_\d+.txt", o["Key"])
   if typeof(m) != Void && length(m.captures) > 0
       seg_1 = parse(Int64, m[1])
       seg_2 = parse(Int64, m[2])
       pair = (seg_1, seg_2)
       if !haskey(pairs, pair)
           pairs[pair] = String[]
       end
       push!(pairs[pair], o["Key"])
   end
end

println("Processing $(length(pairs)) edges via lambda")

num_edges_processed = 0
sum_single_elapsed_time = 0
aff_threshold = Float32(0.25)

processing_time_sums = Dict{String, Float32}(
                            "get_times" => 0,
                            "parse_times" => 0,
                            "load_all_data_time" => 0,
                            "affinity_calculation_time" => 0,
                            "set_time" => 0,
                            "total_time" => 0)

time_begin = now()
for pair in keys(pairs)
    return_data = create_edge_lambda(pair[1],
                                     pair[2],
                                     "seunglab-test",
                                     pairs[pair],
                                     aff_threshold)
    processing_time_sums["get_times"] +=
        sum(return_data["get_times"]) /
        length(return_data["get_times"])
    processing_time_sums["parse_times"] +=
        sum(return_data["parse_times"]) /
        length(return_data["parse_times"])
    processing_time_sums["load_all_data_time"] +=
        return_data["load_all_data_time"]
    processing_time_sums["affinity_calculation_time"] +=
        return_data["affinity_calculation_time"]
    processing_time_sums["set_time"] += return_data["set_time"]
    processing_time_sums["total_time"] += return_data["total_time"]

    num_edges_processed = num_edges_processed + 1
    println("Processed:\t$num_edges_processed")
    println("*************************")
    println("Data:\t$(return_data["data"])")

    println("get_times:\t$(return_data["get_times"])")
    println("Total get_time:\t" *
        "$(sum(return_data["get_times"]))")
    println("Average get_times:\t" *
            "$(processing_time_sums["get_times"] / num_edges_processed)")

    println("parse_times:\t$(return_data["parse_times"])")
    println("Total parse_times:\t" * "$(sum(return_data["parse_times"]))")
    println("Average parse_times:\t" *
            "$(processing_time_sums["parse_times"] / num_edges_processed)")

    println("load_all_data_time:\t" *
            "$(return_data["load_all_data_time"])")
    println("Average load_all_data_time:\t" *
            "$(processing_time_sums["load_all_data_time"] / num_edges_processed)")

    println("affinity_calculation_time:\t" *
            "$(return_data["affinity_calculation_time"])")
    println("Average affinity_calculation_time:\t" *
            "$(processing_time_sums["affinity_calculation_time"] /
               num_edges_processed)")

    println("set_time:\t$(return_data["set_time"])")
    println("Average set_time:\t" *
            "$(processing_time_sums["set_time"] / num_edges_processed)")

    println("total_time:\t$(return_data["total_time"])")
    println("Average total_time:\t" *
            "$(processing_time_sums["total_time"] / num_edges_processed)")

    println("*************************")
end

println("---------------------------------")
println("Total elapsed time: $(now() - time_begin)")
println("Edges processed: $(length(pairs))")
