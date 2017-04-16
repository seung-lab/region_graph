module RegionGraphS3Lambda

using AWSS3
using AWSCore
using RegionGraph

export create_edge

__precompile__()

function create_edge{Ts, Ta}(seg_1::Ts, seg_2::Ts, bucket::String,
                             s3_keys::Array{String, 1}, aff_threshold::Ta)
    aws = AWSCore.aws_config()

    total_time_start = now() # Bench

    p = minmax(seg_1, seg_2)
    edge = MeanEdge{Ta}(zero(UInt32),
                             zero(Ta),
                             Dict{Tuple{Int32,Int32,Int32}, Ta}[
                                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                                Dict{Tuple{Int32,Int32,Int32}, Ta}()]
                            )
    s3_get_times = [] # Bench
    load_all_data_time_start = now() # Bench
    @sync map(s3_keys) do s3_key
        local_filename = "/tmp/" * replace(s3_key, "/", "")
        s3_get_time_start = now() # Bench
        input = s3_get_file(aws, bucket, s3_key, local_filename)
        push!(s3_get_times, now() - s3_get_time_start) # Bench
        @async open(local_filename, "r") do local_file
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
    load_all_data_time = now() - load_all_data_time_start

    affinity_calculation_time_start = now() # Bench

    total_boundaries = union(Set(keys(edge.boundaries[1])),
                             Set(keys(edge.boundaries[2])),
                             Set(keys(edge.boundaries[3])))
    area, sum_affinity = calculate_mean_affinity(edge.boundaries,
                                                 total_boundaries)
    edge.sum_affinity = sum_affinity
    edge.area = area

    cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)
    if length(cc_means) > 0
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) " *
               "$(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)"
    else
        data = "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) " *
               "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)"
    end
    affinity_calculation_time = now() - affinity_calculation_time_start # Bench

    s3_put_time_start = now() # Bench
    s3_put(aws, bucket, "region_graph/output/$(seg_1)_$(seg_2)", data)
    s3_put_time = now() - s3_put_time_start # Bench

    total_time = now() - total_time_start # Bench
    
    returnDict = Dict(
                      "data" => data,
                      "s3_get_times" => s3_get_times,
                      "load_all_data_time" => load_all_data_time,
                      "affinity_calculation_time" => affinity_calculation_time,
                      "s3_put_time" => s3_put_time,
                      "total_time" => total_time)
    return returnDict
end

end # module RegionGraphS3Lambda
