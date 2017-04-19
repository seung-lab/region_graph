include("../src/RegionGraph.jl")

module SQLEdges
    using EMIRT
    using RegionGraph
    using MySQL

    export get_connection, get_incomplete_edge, get_incomplete_edges, insert_incomplete_edge, insert_incomplete_edges

    connection = nothing

    function get_connection()
        global connection
        if connection == nothing
            connection = mysql_connect("rg-test.cxbfj3vur7ww.us-east-1.rds.amazonaws.com", "seunglab", "******", "region_graph")
        end
        return connection
    end

    function get_incomplete_edges{Ta, Ts}(segment_pairs::Array{ Tuple{Ts, Ts}, 1 }, aff_threshold::Ta)
        edges = Dict{ Tuple{Ts, Ts}, MeanEdge{Ts, Ta} }()

        const con = get_connection()

        for segment_pairs_offset in 1:5000:length(segment_pairs)
            segment_pairs_chunk = view(segment_pairs, segment_pairs_offset:min(length(segment_pairs), segment_pairs_offset + 5000 - 1))

            command = string("SELECT seg_id_1, seg_id_2, voxels FROM edge_voxel_list WHERE seg_id_1 = ? AND seg_id_2 = ?",
                              repeat(" UNION SELECT seg_id_1, seg_id_2, voxels FROM edge_voxel_list WHERE seg_id_1 = ? AND seg_id_2 = ?", length(segment_pairs_chunk) - 1),
                              ";")
            querytypes = fill(MYSQL_TYPE_LONGLONG, 2 * length(segment_pairs_chunk))
            queryvalues = reinterpret(typeof(segment_pairs[1][1]), segment_pairs_chunk[:])

            mysql_stmt_prepare(con, command)
            for (seg_id_1, seg_id_2, voxels::Array{UInt8,1}) in MySQLRowIterator(con, querytypes, queryvalues)
                edge = get(edges, tuple(seg_id_1, seg_id_2), MeanEdge{Ts, Ta}(seg_id_1, seg_id_2, zero(Float32), zero(Ta), Dict{Tuple{Int32,Int32,Int32}, Ta}[
                    Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                    Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                    Dict{Tuple{Int32,Int32,Int32}, Ta}()
                ]))

                for offset in 1:17:length(voxels)
                    i = UInt8(voxels[offset+0])
                    x,y,z = reinterpret(Int32, voxels[offset+1:offset+12])
                    aff = reinterpret(Float32, voxels[offset+13:offset+16])[1]
                    coord = (x::Int32,y::Int32,z::Int32)
                    edge.area += 1
                    edge.sum_affinity += aff
                    edge.boundaries[i][coord] = aff
                end

                edges[tuple(seg_id_1, seg_id_2)] = edge
            end
        end

        return edges
    end

    function get_incomplete_edge{Ta, Ts}(segment_pair::Tuple{Ts, Ts}, aff_threshold::Ta)
        const res = get_incomplete_edges([segment_pair], aff_threshold)
        try
            return res[segment_pair]
        catch
            warn("Edge $(segment_pair[1]) <-> $(segment_pair[2]) does not exist.")
            empty = MeanEdge{Ts, Ta}(segment_pair[1], segment_pair[2], zero(Float32), zero(Ta), Dict{Tuple{Int32,Int32,Int32}, Ta}[
                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                Dict{Tuple{Int32,Int32,Int32}, Ta}()
            ])

            return empty
        end
    end

    function insert_incomplete_edges{Ta, Ts}(edges::Array{MeanEdge{Ts, Ta}, 1}, startIndex::Array{Int32, 1})
        (xstart::Int32, ystart::Int32, zstart::Int32) = startIndex[1:3]
        const con = get_connection()

        for edges_offset in 1:5000:length(edges)
            edges_chunk = view(edges, edges_offset:min(length(edges), edges_offset + 5000 - 1))

            command = string("INSERT INTO edge_voxel_list (seg_id_1,seg_id_2,chunk_id,voxels) VALUES (?,?,?,?)",
                              repeat(",(?,?,?,?)", length(edges_chunk) - 1),
                              ";")
            querytypes = repmat([MYSQL_TYPE_LONGLONG, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_LONG_BLOB], length(edges_chunk))
            queryvalues = Array{Any, 1}(4 * length(edges_chunk))

            for (idx, edge) in enumerate(edges_chunk)
                hexBuffer = IOBuffer();
                for i in 1:3
                    for k in keys(edge.boundaries[i])
                        write(hexBuffer, UInt8(i), Int32(k[1]), Int32(k[2]), Int32(k[3]), Float32(edge.boundaries[i][k]))
                    end
                end
                seekstart(hexBuffer)
                queryvalues[(4*idx - 3)] = edge.seg_id_1
                queryvalues[(4*idx - 2)] = edge.seg_id_2
                queryvalues[(4*idx - 1)] = "$(xstart)_$(ystart)_$(zstart)"
                queryvalues[(4*idx - 0)] = read(hexBuffer)
            end

            mysql_stmt_prepare(con, command)
            mysql_execute(con, querytypes, queryvalues)
        end
    end

    function insert_incomplete_edge{Ta, Ts}(edge::MeanEdge{Ts, Ta}, startIndex::Array{Int32, 1})
        insert_incomplete_edges([edge], startIndex)
    end

end