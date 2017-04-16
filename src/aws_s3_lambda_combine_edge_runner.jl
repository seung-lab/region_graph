push!(LOAD_PATH, ".")

using AWSCore
using AWSS3
using AWSLambda
using RegionGraph
using RegionGraphS3Lambda
aws = AWSCore.aws_config()

function get_data{Ta,Ts}(p::Tuple{Ts, Ts}, edge::MeanEdge{Ta},
                         aff_threshold::Ta, num::Int, num2::Int)

    affinity_calculation_time = now() # Bench
    total_boundaries = union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3])))
    area, sum_affinity = calculate_mean_affinity(edge.boundaries, total_boundaries)
    edge.sum_affinity = sum_affinity
    edge.area = area

    cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)

    if length(cc_means) > 0
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)\n"
    else
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)\n"
    end
    println("$p\t$num\t$num2\t$(now() - affinity_calculation_time)")
    return data
end

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

num_processed = 0
sum_single_elapsed_time = 0

processing_time_sums = Dict{String, Float32}(
                            "s3_get_times" => 0,
                            "load_all_data_time" => 0,
                            "affinity_calculation_time" => 0,
                            "s3_put_time" => 0,
                            "total_time" => 0)

aff_threshold = Float32(0.25)
time_begin = now()
for pair in keys(pairs)
    return_data = create_edge_lambda(pair[1],
                                     pair[2],
                                     "seunglab-test",
                                     pairs[pair],
                                     aff_threshold)
    processing_time_sums["s3_get_times"] +=
        sum(map((ms) -> Float32(ms.value), return_data["s3_get_times"])) /
        length(return_data["s3_get_times"])
    processing_time_sums["load_all_data_time"] +=
        return_data["load_all_data_time"].value
    processing_time_sums["affinity_calculation_time"] +=
        return_data["affinity_calculation_time"].value
    processing_time_sums["s3_put_time"] += return_data["s3_put_time"].value
    processing_time_sums["total_time"] += return_data["total_time"].value

    num_processed = num_processed + 1
    println("Processed: $num_processed")
    println("*************************")
    println("Data: $(return_data["data"])")

    println("s3_get_time: $(return_data["s3_get_times"])")
    println("s3_get_time_total: " *
        "$(sum(map((ms) -> Float32(ms.value), return_data["s3_get_times"])))")
    println("Average s3_get_times: " *
            "$(processing_time_sums["s3_get_times"] / num_processed)")

    println("load_all_data_time: $(return_data["load_all_data_time"])")
    println("Average load_all_data_time:" *
            "$(processing_time_sums["load_all_data_time"] / num_processed)")
    
    println("affinity_calculation_time:" *
            "$(return_data["affinity_calculation_time"])")
    println("Average affinity_calculation_time:" *
            "$(processing_time_sums["affinity_calculation_time"] / num_processed)")

    println("s3_put_time: $(return_data["s3_put_time"])")
    println("Average s3_put_time:" *
            "$(processing_time_sums["s3_put_time"] / num_processed)")

    println("total_time: $(return_data["total_time"])")
    println("Average total_time:" *
            "$(processing_time_sums["total_time"] / num_processed)")

    println("*************************")
end

println("---------------------------------")
println("Total elapsed time: $(now() - time_begin)")
println("Edges processed: $(length(pairs))")
