push!(LOAD_PATH, ".")

using AWSCore
using AWSS3
using AWSLambda
using RegionGraph
aws = AWSCore.aws_config()

# get the keys ahead of time so we don't have to pay for additional query
# overhead
create_edge_lambda = @lambda aws function create_edge(seg_1::Int32,
                                                      seg_2::Int32,
                                                      bucket::String,
                                                      s3_keys::Array{String, 1},
                                                      aff_threshold::Float32)
    #@assert !Base.stale_cachefile("/var/task/julia/RegionGraph/RegionGraph.jl",
                                  #Base.LOAD_CACHE_PATH[1] * "/RegionGraph.ji")

    using AWSS3
    using AWSCore
    using RegionGraph

    aws = AWSCore.aws_config()

    total_time = now() # Bench

    p = minmax(seg_1, seg_2)
    edge = MeanEdge{Float32}(zero(UInt32),
                             zero(Float64),
                             Dict{Tuple{Int32,Int32,Int32}, Float32}[
                                Dict{Tuple{Int32,Int32,Int32}, Float32}(),
                                Dict{Tuple{Int32,Int32,Int32}, Float32}(),
                                Dict{Tuple{Int32,Int32,Int32}, Float32}()]
                            )
    s3_get_times = [] # Bench
    load_all_data_time = now() # Bench
    map(s3_keys) do s3_key
        local_filename = "/tmp/" * replace(s3_key, "/", "")
        s3_get_time = now() # Bench
        input = s3_get_file(aws, bucket, s3_key, local_filename)
        push!(s3_get_times, now() - s3_get_time) # Bench
        open(local_filename, "r") do local_file
            #load_voxels(local_file, edge)
            for line in eachline(local_file)
                data = split(line, " ")
                i = parse(Int, data[1])
                x,y,z = [parse(Int32, x) for x in data[2:4]]
                aff = parse(Float32, data[5])
                coord = (x::Int32, y::Int32, z::Int32)
                edge.boundaries[i][coord] = aff
            end

        end
    end
    load_all_data_time = now() - load_all_data_time

    affinity_calculation_time = now() # Bench

    total_boundaries = union(Set(keys(edge.boundaries[1])),
                             Set(keys(edge.boundaries[2])),
                             Set(keys(edge.boundaries[3])))
    area, sum_affinity = calculate_mean_affinity(edge.boundaries,
                                                 total_boundaries)
    edge.sum_affinity = sum_affinity
    edge.area = area

    cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)
    affinity_calculation_time = now() - affinity_calculation_time # Bench
    if length(cc_means) > 0
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) " *
               "$(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)\n"
    else
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) " *
               "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)\n"
    end
    s3_put_time = now() # Bench
    s3_put(aws, bucket, "region_graph/output/$(seg_1)_$(seg_2)", data)
    s3_put_time = now() - s3_put_time # Bench

    total_time = now() - total_time # Bench
    return "Data: $data\n" *
           "S3 Get: $s3_get_times\n" *
           "Load All Data: $load_all_data_time\n" *
           "Affinity Calculation: $affinity_calculation_time\n" *
           "S3 Put: $s3_put_time\n" *
           "-----TOTAL-----: $total_time"
end

string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)
string = @time create_edge_lambda(
                  Int32(172292),
                  Int32(296301),
                  "seunglab-test",
                  [
                      "region_graph/edges/172292_296301_256_0_0.txt",
                      "region_graph/edges/172292_296301_0_0_0.txt"
                  ],
                  Float32(0.25))
println(string)

