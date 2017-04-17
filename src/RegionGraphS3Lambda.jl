module RegionGraphS3Lambda

using AWSS3
using AWSCore
using RegionGraph

export create_edge, create_edges

__precompile__()

function create_edge{Ts, Ta}(seg_1::Ts, seg_2::Ts, bucket::String,
                             s3_keys::Array{String, 1}, aff_threshold::Ta)
    aws = AWSCore.aws_config()

    total_time_start = now() # Bench

    p = minmax(seg_1, seg_2)
    edge = MeanEdge{Ta}(zero(UInt32),
                             zero(Ta),
                             Dict{Tuple{Ts,Ts,Ts}, Ta}[
                                Dict{Tuple{Ts,Ts,Ts}, Ta}(),
                                Dict{Tuple{Ts,Ts,Ts}, Ta}(),
                                Dict{Tuple{Ts,Ts,Ts}, Ta}()]
                            )
    get_times = [] # Bench
    parse_times = [] # Bench

    load_all_data_time = @elapsed map(s3_keys) do s3_key
        local_filename = "/tmp/" * replace(s3_key, "/", "")
        get_time = @elapsed input = 
            s3_get_file(aws, bucket, s3_key, local_filename)
        parse_time = @elapsed open(local_filename, "r") do local_file
            for line in eachline(local_file)
                data = split(line, " ")
                i = parse(Int, data[1])
                x,y,z = [parse(Ts, x) for x in data[2:4]]
                aff = parse(Ta, data[5])
                coord = (x::Ts, y::Ts, z::Ts)
                edge.boundaries[i][coord] = aff
            end
        end
        push!(get_times, get_time) # Bench
        push!(parse_times, parse_time) # Bench
    end

    affinity_calculation_time = @elapsed begin

        total_boundaries = union(Set(keys(edge.boundaries[1])),
                                 Set(keys(edge.boundaries[2])),
                                 Set(keys(edge.boundaries[3])))
        area, sum_affinity = calculate_mean_affinity(edge.boundaries,
                                                     total_boundaries)
        edge.sum_affinity = sum_affinity
        edge.area = area

        cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)
        data = ""
        if length(cc_means) > 0
            data = join([
                         p[0], p[2], Float64(edge.sum_affinity), edge.area,
                         p[1], p[2], maximum(cc_means),          edge.area
                        ], " ")
        else
            data = join([
                         p[1], p[2], Float64(edge.sum_affinity), edge.area,
                         p[1], p[2], Float64(edge.sum_affinity), edge.area
                         ], " ")
        end
    end

    set_time = @elapsed s3_put(
        aws, bucket, "region_graph/output/$(seg_1)_$(seg_2)", data)

    total_time = now() - total_time_start # Bench
    
    return Dict("data" => data,
                "get_times" => get_times * 1000,
                "parse_times" => parse_times * 1000,
                "load_all_data_time" => load_all_data_time * 1000,
                "affinity_calculation_time" =>
                    affinity_calculation_time * 1000,
                "set_time" => set_time * 1000,
                "total_time" => total_time.value)
end

function create_edges{Ts, Ta}(
        batched_pairs::Dict{Tuple{Ts, Ts}, Array{String, 1}},
        bucket::String,
        aff_threshold::Ta)
    aws = AWSCore.aws_config()

    edges = Dict{Tuple{Ts, Ts}, MeanEdge{Float32}}()

    total_time = now() # Bench
    get_times = 0 # Bench
    parse_times = 0 # Bench

    load_all_data_time = @elapsed map(keys(batched_pairs)) do pair
        if !haskey(edges, pair)
            edges[pair] = MeanEdge{Ta}(zero(UInt32),
                 zero(Ta),
                 Dict{Tuple{Ts,Ts,Ts}, Ta}[
                    Dict{Tuple{Ts,Ts,Ts}, Ta}(),
                    Dict{Tuple{Ts,Ts,Ts}, Ta}(),
                    Dict{Tuple{Ts,Ts,Ts}, Ta}()]
                )
        end
        edge = edges[pair]
        map(batched_pairs[pair]) do s3_key
            local_filename = "/tmp/" * replace(s3_key, "/", "")
            get_times += @elapsed input =
                s3_get_file(aws, bucket, s3_key, local_filename)
            parse_times += @elapsed open(local_filename, "r") do local_file
                for line in eachline(local_file)
                    data = split(line, " ")
                    i = parse(Int, data[1])
                    x,y,z = [parse(Ts, x) for x in data[2:4]]
                    aff = parse(Ta, data[5])
                    coord = (x::Ts, y::Ts, z::Ts)
                    edge.boundaries[i][coord] = aff
                end
            end
        end
    end

    data = []
    affinity_calculation_time = 0
    set_times = 0
    map(keys(edges)) do pair

        affinity_calculation_time += @elapsed begin
            
            edge = edges[pair]

            affinity_calculation_time = now() # Bench

            total_boundaries = union(Set(keys(edge.boundaries[1])),
                                     Set(keys(edge.boundaries[2])),
                                     Set(keys(edge.boundaries[3])))
            area, sum_affinity = calculate_mean_affinity(edge.boundaries,
                                                         total_boundaries)
            edge.sum_affinity = sum_affinity
            edge.area = area

            cc_means = calculate_mean_affinity_pluses(pair, edge, aff_threshold)
        end

        if length(cc_means) > 0
            push!(data, join([
                      pair[1], pair[2], Float64(edge.sum_affinity), edge.area,
                      pair[1], pair[2], maximum(cc_means),          edge.area
                     ], " "))
        else
            push!(data, join([
                      pair[1], pair[2], Float64(edge.sum_affinity), edge.area,
                      pair[1], pair[2], Float64(edge.sum_affinity), edge.area
                     ], " "))
        end

        set_times += @elapsed s3_put(
           aws, bucket, "region_graph/output/$(pair[1])_$(pair[2])",
           join(data, "\n"))
    end

    total_time = now() - total_time # Bench
    
    return Dict("data" => data,
                "get_times" => get_times * 1000,
                "parse_times" => parse_times * 1000,
                "load_all_data_time" => load_all_data_time * 1000,
                "affinity_calculation_time" =>
                    affinity_calculation_time * 1000,
                "set_times" => set_times * 1000,
                "total_time" => total_time.value)
end

end # module RegionGraphS3Lambda
