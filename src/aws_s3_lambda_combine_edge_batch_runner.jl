@everywhere push!(LOAD_PATH, ".")

using AWSCore
using AWSS3
# using AWSLambda
using RegionGraph
using RegionGraphS3Lambda
aws = AWSCore.aws_config()

# get the keys ahead of time so we don't have to pay for additional query
# overhead
create_edge_lambda_batch = function create_edge_jl_batch(
        batched_pairs::Dict{Tuple{Int64, Int64}, Array{String, 1}},
        bucket::String,
        aff_threshold::Float32)
    #@assert !Base.stale_cachefile("/var/task/julia/RegionGraph/RegionGraph.jl",
                                  #Base.LOAD_CACHE_PATH[1] * "/RegionGraph.ji")

#     using AWSS3
#     using AWSCore
#     using RegionGraph
#     using RegionGraphS3Lambda

    return create_edges(batched_pairs, bucket, aff_threshold)
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

aff_threshold = Float32(0.25)
sum_single_elapsed_time = 0
num_edges_processed = 0
num_batches_processed = 0
BATCH_SIZE = 100

processing_time_sums = Dict{String, Float32}(
                            "get_times" => 0,
                            "parse_times" => 0,
                            "load_all_data_time" => 0,
                            "affinity_calculation_time" => 0,
                            "set_times" => 0,
                            "total_time" => 0)

time_begin = now()
batched_pairs = Dict{Tuple{Int64, Int64}, Array{String, 1}}()
for pair in keys(pairs)
    batch_index = num_edges_processed % BATCH_SIZE 
    if batch_index == 0
        batched_pairs = Dict{Tuple{Int64, Int64}, Array{String, 1}}()
    end

    batched_pairs[pair] = pairs[pair]
    num_edges_processed = num_edges_processed + 1

    if batch_index == BATCH_SIZE - 1 || num_edges_processed == length(pairs)
        num_batches_processed += 1
        num_edges_processed_in_batch = batch_index + 1
        return_data = create_edge_lambda_batch(batched_pairs,
                                         "seunglab-test",
                                         aff_threshold)
        processing_time_sums["get_times"] += return_data["get_times"]
        processing_time_sums["parse_times"] += return_data["parse_times"]
        processing_time_sums["load_all_data_time"] +=
            return_data["load_all_data_time"]
        processing_time_sums["affinity_calculation_time"] +=
            return_data["affinity_calculation_time"]
        processing_time_sums["set_times"] += return_data["set_times"]
        processing_time_sums["total_time"] += return_data["total_time"]

        num_edges_processed = num_edges_processed + 1
        println("Processed:\t$num_edges_processed")
        println("*************************")
        println("Data ---- ")
        foreach(return_data["data"]) do data
            println(data[2])
        end
        println("END Data ---- ")

        println("Batch get_times:\t$(return_data["get_times"])")
        println("Batch Average get_times:\t" *
                "$(processing_time_sums["get_times"] / num_batches_processed)")
        println("\tSingle Average get_times:\t" *
                "$(processing_time_sums["get_times"] / num_edges_processed)")

        println("Batch parse_times:\t$(return_data["parse_times"])")
        println("Batch Average parse_times:\t" *
                "$(processing_time_sums["parse_times"] /
                   num_batches_processed)")
        println("\tSingle Average parse_times:\t" *
                "$(processing_time_sums["parse_times"] / num_edges_processed)")

        println("Batch load_all_data_time:\t" *
                "$(return_data["load_all_data_time"])")
        println("Batch Average load_all_data_time:\t" *
                "$(processing_time_sums["load_all_data_time"] /
                   num_batches_processed)")
        println("\tSingle Average load_all_data_time:\t" *
                "$(processing_time_sums["load_all_data_time"] /
                   num_edges_processed)")

        println("Batch affinity_calculation_time:\t" *
                "$(return_data["affinity_calculation_time"]) ******")
        println("Batch Average affinity_calculation_time:\t" *
                "$(processing_time_sums["affinity_calculation_time"] /
                   num_batches_processed) ******")
        println("\tSingle Average affinity_calculation_time:\t" *
                "$(processing_time_sums["affinity_calculation_time"] /
                   num_edges_processed) ******")

        println("Batch set_time:\t$(return_data["set_times"])")
        println("Batch Average set_time:\t" *
                "$(processing_time_sums["set_times"] / num_batches_processed)")
        println("\tSingle Average set_time:\t" *
                "$(processing_time_sums["set_times"] / num_edges_processed)")

        println("Batch total_time:\t$(return_data["total_time"])")
        println("Batch Average total_time:\t" *
                "$(processing_time_sums["total_time"] /
                   num_edges_processed_in_batch)")
        println("\tSingle Average total_time:\t" *
                "$(processing_time_sums["total_time"] / num_edges_processed)")

        println("*************************")
    end
end

println("---------------------------------")
println("Total elapsed time: $(now() - time_begin)")
println("Edges processed: $(length(pairs))")
