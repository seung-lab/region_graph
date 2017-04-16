include("../src/RegionGraph.jl")

using HDF5
using BigArrays
using BigArrays.H5sBigArrays
using EMIRT
using RegionGraph
using Blosc
using MySQL

con = mysql_connect("rg-test.cxbfj3vur7ww.us-east-1.rds.amazonaws.com", "seunglab", "******", "region_graph")


function write_txt{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,offset::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=offset

    println("Calculating connect components")
    complete_edge_file = open("txt/rg_volume_$(xstart)_$(ystart)_$(zstart).in","w")

    count_edges = 0
    boundary_edges = Set{Tuple{Ts,Ts}}()
    for p in keys(edges)
        edge = edges[p]
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            open("txt/$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).txt", "w") do incomplete_edge_file
                for i in 1:3
                    for k in keys(edge.boundaries[i])
                        write(incomplete_edge_file, "$i $(k[1]+xstart) $(k[2]+ystart) $(k[3]+zstart) $(Float64(edge.boundaries[i][k]))\n")
                    end
                end
            end
            count_edges+=1
            continue
        end
            
        total_boundaries = union(Set(keys(edge.boundaries[1])),Set(keys(edge.boundaries[2])),Set(keys(edge.boundaries[3])))
        area, sum_affinity = calculate_mean_affinity(edge.boundaries, total_boundaries)
        edge.sum_affinity = sum_affinity
        edge.area = area
        cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)

        if length(cc_means) > 0
            write(complete_edge_file, "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(maximum(cc_means)) $(edge.area)\n")
        else
            write(complete_edge_file, "$(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area) $(p[1]) $(p[2]) $(Float64(edge.sum_affinity)) $(edge.area)\n")
        end
    end
    close(complete_edge_file)

    println("boundary segments: $(length(boundary_edges)), edges: $(count_edges)")
    open("txt/incomplete_edges_$(xstart)_$(ystart)_$(zstart).txt", "w") do f
        for p in boundary_edges
            write(f, "$(p[1]) $(p[2])\n")
        end
    end
end

function write_sql{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,startIndex::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=startIndex

    

    println("Calculating connected components")
    boundary_edges = Set{Tuple{Ts,Ts}}()

    incomplete_edges_file = open("sql/incomplete_edges.sql","w");
    incomplete_edge_count = 0;
    incomplete_edge_inserts = 100;
    incomplete_edge_buffer = IOBuffer();

    complete_edges_file = open("sql/complete_edges.sql","w");
    complete_edge_count = 0;
    complete_edge_inserts = 5000;
    complete_edge_buffer = IOBuffer();

    for p in keys(edges)
        edge = edges[p]
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            if incomplete_edge_count % incomplete_edge_inserts == 0
                incomplete_edge_buffer = IOBuffer();
                write(incomplete_edge_buffer, "INSERT INTO edge_voxel_list (seg_id_1,seg_id_2,chunk_id,voxels) VALUES ")
            else
                write(incomplete_edge_buffer, ",")
            end

            write(incomplete_edge_buffer, "($(p[1]),$(p[2]),\"$(xstart)_$(ystart)_$(zstart)\",0x")

            hexBuffer = IOBuffer();
            for i in 1:3
                for k in keys(edge.boundaries[i])
                    write(hexBuffer, UInt8(i), Int32(k[1]), Int32(k[2]), Int32(k[3]), Float32(edge.boundaries[i][k]))
                end
            end
            seekstart(hexBuffer)
            write(incomplete_edge_buffer, map(char->hex(char, 2), read(hexBuffer)), ")")

            if incomplete_edge_count % incomplete_edge_inserts == incomplete_edge_inserts-1
                write(incomplete_edge_buffer, ";\n")
                seekstart(incomplete_edge_buffer);
                res = mysql_execute(con, convert(String, read(incomplete_edge_buffer)))
                #write(incomplete_edges_file, read(incomplete_edge_buffer))
            end

            incomplete_edge_count+=1
        else

            if complete_edge_count % complete_edge_inserts == 0
                complete_edge_buffer = IOBuffer();
                write(complete_edge_buffer, "INSERT INTO edge (seg_id_1,seg_id_2,area,sum_affinity,max_edge_affinity) VALUES ")
            else
                write(complete_edge_buffer, ",")
            end

            total_boundaries = union(Set(keys(edge.boundaries[1])), Set(keys(edge.boundaries[2])), Set(keys(edge.boundaries[3])))
            area, sum_affinity = calculate_mean_affinity(edge.boundaries, total_boundaries)
            edge.sum_affinity = sum_affinity
            edge.area = area
            cc_means = calculate_mean_affinity_pluses(p, edge, aff_threshold)

            if length(cc_means) > 0
                write(complete_edge_buffer, "($(p[1]),$(p[2]),$(Float64(edge.area)),$(Float64(edge.sum_affinity)),$(Float64(maximum(cc_means))))")
            else
                write(complete_edge_buffer, "($(p[1]),$(p[2]),$(Float64(edge.area)),$(Float64(edge.sum_affinity)),$(Float64(edge.sum_affinity)))")
            end

            if complete_edge_count % complete_edge_inserts == complete_edge_inserts-1
                write(complete_edge_buffer, ";\n")
                seekstart(complete_edge_buffer);
                res = mysql_execute(con, convert(String, read(complete_edge_buffer)))
                #write(complete_edges_file, read(complete_edge_buffer))
            end

            complete_edge_count+=1
        end
    end
end

function write_custombinary{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,startIndex::Array{Int32,1})

    (xstart::Int32,ystart::Int32,zstart::Int32)=startIndex

    println("Calculating connect components")
    count_edges = 0
    boundary_edges = Set{Tuple{Ts,Ts}}()
    for p in keys(edges)
        edge = edges[p]
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            open("bin/$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).bin", "w") do incomplete_edge_file
                ioBuffer = IOBuffer();
                for i in 1:3
                    for k in keys(edge.boundaries[i])
                        write(ioBuffer, UInt8(i))
                        write(ioBuffer, Int32(k[1]))
                        write(ioBuffer, Int32(k[2]))
                        write(ioBuffer, Int32(k[3]))
                        write(ioBuffer, Float32(edge.boundaries[i][k]))
                    end
                end
                seekstart(ioBuffer)
                write(incomplete_edge_file, read(ioBuffer))
            end
            count_edges+=1
            continue
        end
    end
end

function read_custombinary()
    edges = Dict{Tuple{UInt32,UInt32}, MeanEdge{Float32}}()

    for filename in readdir("./bin")
        p = tuple(map(s->parse(UInt32, s), split(splitext(basename(filename))[1], "_"))[1:2]...)

        f = open("./bin/$(filename)", "r")
        buf = read(f);
        close(f)

        edge = MeanEdge{Float32}(zero(UInt32),zero(Float32),Dict{Tuple{Int32,Int32,Int32}, Float32}[Dict{Tuple{Int32,Int32,Int32}, Float32}(),Dict{Tuple{Int32,Int32,Int32}, Float32}(),Dict{Tuple{Int32,Int32,Int32}, Float32}()])

        for offset in 1:17:length(buf)
            i = UInt8(buf[offset+0])
            x,y,z = reinterpret(Int32, buf[offset+1:offset+12])
            aff = reinterpret(Float32, buf[offset+13:offset+16])[1]
            coord = (x::Int32,y::Int32,z::Int32)
            edge.area += 1
            edge.sum_affinity += aff
            edge.boundaries[i][coord] = aff
        end

        edges[p] = edge
    end
    #println(edges[(478965,479263)])
end

function write_custombinaryBlosc9{Ta,Ts}(edges::Dict{Tuple{Ts,Ts},MeanEdge{Ta}},incomplete_segments::Set{Ts},aff_threshold::Ta,startIndex::Array{Int32,1})
    (xstart::Int32,ystart::Int32,zstart::Int32)=startIndex


    println("Calculating connect components")
    count_edges = 0
    boundary_edges = Set{Tuple{Ts,Ts}}()
    for p in keys(edges)
        edge = edges[p]
        if p[1] in incomplete_segments && p[2] in incomplete_segments
            push!(boundary_edges, p)
            open("blosc9/$(p[1])_$(p[2])_$(xstart)_$(ystart)_$(zstart).bin", "w") do incomplete_edge_file
                ioBuffer = IOBuffer();
                for i in 1:3
                    for k in keys(edge.boundaries[i])
                        write(ioBuffer, UInt8(i))
                        write(ioBuffer, Int32(k[1]))
                        write(ioBuffer, Int32(k[2]))
                        write(ioBuffer, Int32(k[3]))
                        write(ioBuffer, Float32(edge.boundaries[i][k]))
                    end
                end
                seekstart(ioBuffer)
                write(incomplete_edge_file, compress(read(ioBuffer); level=9))
            end
            count_edges+=1
            continue
        end
    end
end

function read_custombinaryBlosc9()
    edges = Dict{Tuple{UInt32,UInt32}, MeanEdge{Float32}}()

    for filename in readdir("./blosc9")
        p = tuple(map(s->parse(UInt32, s), split(splitext(basename(filename))[1], "_"))[1:2]...)

        f = open("./blosc9/$(filename)", "r")
        buf = decompress(UInt8, read(f));
        close(f)

        edge = MeanEdge{Float32}(zero(UInt32),zero(Float32),Dict{Tuple{Int32,Int32,Int32}, Float32}[Dict{Tuple{Int32,Int32,Int32}, Float32}(),Dict{Tuple{Int32,Int32,Int32}, Float32}(),Dict{Tuple{Int32,Int32,Int32}, Float32}()])

        for offset in 1:17:length(buf)
            i = UInt8(buf[offset+0])
            x,y,z = reinterpret(Int32, buf[offset+1:offset+12])
            aff = reinterpret(Float32, buf[offset+13:offset+16])[1]
            coord = (x::Int32,y::Int32,z::Int32)
            edge.area += 1
            edge.sum_affinity += aff
            edge.boundaries[i][coord] = aff
        end

        edges[p] = edge
    end
    #println(edges[(478965,479263)])
end

function main()

    # command = "SHOW VARIABLES LIKE \"max%\";"
    # res = mysql_execute(con, command)
    # print(res)
    # mysql_disconnect(con)
    # exit(0)

    aff_in = H5sBigArray("../affinitymap/");
    aff = AffinityMap(aff_in[29697:30720, 24577:25600, 1:1024, 1:3])

    seg_in = H5sBigArray("../segmentation/");
    seg = Segmentation(seg_in[29697:30720, 24577:25600, 1:1024])

    aff_threshold = Float32(0.25)

    data_size = Int32[1024, 1024, 1024]
    chunk_size = Int32[1024, 1024, 1024]
    dilation_size = Int32[1,1,0]

    Blosc.set_compressor("blosclz")
    Blosc.set_num_threads(8)


    z = 1
    for x in 1:chunk_size[1]:data_size[1]
        for y in 1:chunk_size[2]:data_size[2]
            global_offset = Int32[x, y, 1].-1 # -1 because julia
            end_index = global_offset.+chunk_size.+dilation_size

            if end_index[1] >= data_size[1]
                end_index[1] = data_size[1]
            end
            if end_index[2] >= data_size[2]
                end_index[2] = data_size[2]
            end

            println(global_offset)
            println(end_index)

            aff_view = aff[x:end_index[1],y:end_index[2],z:end_index[3],:]
            seg_view = seg[x:end_index[1],y:end_index[2],z:end_index[3]]

            @time edges, incomplete_segments = enumerate_edges(aff_view,seg_view)
            #@time write_txt(edges,incomplete_segments,aff_threshold,global_offset)
            #println("Writing Binary")
            #@time write_custombinary(edges,incomplete_segments,aff_threshold,global_offset)
            println("Writing SQL")
            @time write_sql(edges,incomplete_segments,aff_threshold,global_offset)
            #println("Writing Blosc")
            #@time write_custombinaryBlosc9(edges,incomplete_segments,aff_threshold,global_offset)

            #println("Reading Binary")
            #@time read_custombinary()
            #println("Reading Blosc")
            #@time read_custombinaryBlosc9()
        end
    end
    mysql_disconnect(con)
end


main()
