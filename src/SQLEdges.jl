include("RegionGraph.jl")

module SQLEdges
using EMIRT
using RegionGraph
using MySQL

import Base.search
export get_connection, get_incomplete_edge, get_incomplete_edges, insert_incomplete_edge, insert_incomplete_edges

connection = nothing
const escape = Dict([0x00] => [0x5C, 0x30], [0x5C] => [0x5C, 0x5C])
const unescape = Dict([0x5C, 0x30] => [0x00] , [0x5C,0x5C] => [0x5C])

"Create a new and/or return an existing MySQL connection"
function get_connection()
    global connection
    if connection == nothing
        connection = mysql_connect("rg-test.cxbfj3vur7ww.us-east-1.rds.amazonaws.com", "rguser", "***********", "region_graph")
    end
    return connection
end

"""
    search(a, patterns, start)

Searches for the first occurrences of any of the given UInt8[] `patterns` in `a`, starting at offset `start`.
Note: Results for overlapping patterns are undetermined.
"""
function search(a::Array{UInt8,1}, patterns::Array{Array{UInt8,1},1}, start::Integer)
    return reduce(function(p1, p2)
        r1 = search(a, p1, start)
        r2 = search(a, p2, start)
        if first(r1) > 0 && first(r2) > 0
            first(r1) < first(r2) ? r1 : r2
        else
            first(r2) == 0 ? r1 : r2
        end
    end, patterns)
end

"""
    bin_replace(arr, patterns, repl)

Replaces all occurrences of given UInt8[] `patterns` in `arr` using the `repl` function in order of their appearance within `arr`.
Note: Results for overlapping patterns are undetermined.
"""
function bin_replace(arr::Array{UInt8,1}, patterns::Array{Array{UInt8,1},1}, repl::Function)
    last_end = arr_start = start(arr)
    arr_end = endof(arr)
    next_range = search(arr, patterns, last_end)
    next_start, next_end = first(next_range), last(next_range)
    out = IOBuffer(Array{UInt8}(floor(Int, 1.2sizeof(arr))), true, true)
    out.size = 0
    out.ptr = 1

    while next_start != 0
        if last_end == arr_start || last_end <= next_end
            unsafe_write(out, pointer(arr, last_end), UInt(next_start - last_end))
            write(out, repl(view(arr, next_start:next_end)))
            last_end = next_start = next_end
        end
        
        if next_start > arr_end
            break
        end

        last_end += 1
        next_range = search(arr, patterns, last_end)
        next_start, next_end = first(next_range), last(next_range)
    end
    write(out, view(arr, last_end:arr_end))
    return resize!(out.data, out.size)
end

"Encode a UInt8 array to store as blob in MySQL. (escapes \0 and \\ characters)"
function mysql_blob_encode(arr::Array{UInt8,1})
    return push!(bin_replace(arr, [[0x00], [0x5C]], c -> escape[c] ), 0x00)
end

"Decode a UInt8 array encoded with `mysql_blob_encode`."
function mysql_blob_decode(arr::Array{UInt8,1})
    return bin_replace(arr, [[0x5C, 0x5C], [0x5C, 0x30]], c -> unescape[c] )
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
        for (seg_id_1, seg_id_2, voxels) in MySQLRowIterator(con, querytypes, queryvalues)
            voxels = Array{UInt8}(voxels)
            edge = get(edges, tuple(seg_id_1, seg_id_2), MeanEdge{Ts, Ta}(seg_id_1, seg_id_2, zero(Float32), zero(Ta), Dict{Tuple{Int32,Int32,Int32}, Ta}[
                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                Dict{Tuple{Int32,Int32,Int32}, Ta}(),
                Dict{Tuple{Int32,Int32,Int32}, Ta}()
            ]))

            voxels = mysql_blob_decode(voxels)

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
    const (xstart::Int32, ystart::Int32, zstart::Int32) = startIndex[1:3]
    const chunk_str = "\"$(xstart)_$(ystart)_$(zstart)\""
    const con = get_connection()
    const max_inserts = 5000
    old_edges_chunk_count = 0
    
    command = String("")
    querytypes = Array{UInt32, 1}()
    queryvalues = Array{Array{UInt8, 1}, 1}()

    for edges_offset in 1:max_inserts:length(edges)
        edges_chunk = view(edges, edges_offset:min(length(edges), edges_offset + max_inserts - 1))
        edges_chunk_count = length(edges_chunk)

        if (edges_chunk_count != old_edges_chunk_count) # Prevent the recreation of the same arrays over and over again
            # command = string("INSERT INTO edge_voxel_list2 (seg_id_1,seg_id_2,chunk_id,voxels) VALUES (?,?,?,?)",
            #                 repeat(",(?,?,?,?)", edges_chunk_count - 1),
            #                 ";")
            command = string("INSERT INTO edge_voxel_list (seg_id_1,seg_id_2,chunk_id,voxels) VALUES ($(edges_chunk[1].seg_id_1),$(edges_chunk[1].seg_id_2),$(chunk_str),?)",
                            map(x -> string(",($(x.seg_id_1),$(x.seg_id_2),$(chunk_str),?)"), view(edges_chunk, 2:length(edges_chunk)))...,
                            ";")
            
            # querytypes = repmat([MYSQL_TYPE_LONGLONG, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_VARCHAR, MYSQL_TYPE_LONG_BLOB], edges_chunk_count)
            # queryvalues = Array{Any, 1}(4 * edges_chunk_count)
            querytypes = repmat([MYSQL_TYPE_LONG_BLOB], edges_chunk_count)
            queryvalues = Array{Array{UInt8, 1}, 1}(1 * edges_chunk_count)
            old_edges_chunk_count = edges_chunk_count
        end

        for (idx, edge) in enumerate(edges_chunk)
            hexBuffer = IOBuffer();
            for i in 1:3
                for k in keys(edge.boundaries[i])
                    write(hexBuffer, UInt8(i), Int32(k[1]), Int32(k[2]), Int32(k[3]), Float32(edge.boundaries[i][k]))
                end
            end
            # queryvalues[(4*idx - 3)] = edge.seg_id_1
            # queryvalues[(4*idx - 2)] = edge.seg_id_2
            # queryvalues[(2*idx - 1)] = chunk_str

            queryvalues[idx] = mysql_blob_encode(resize!(hexBuffer.data, hexBuffer.size))
        end

        mysql_stmt_prepare(con, command)
        res = mysql_execute(con, querytypes, queryvalues)
        println(res, " new edges inserted.")
    end
    return queryvalues
end

function insert_incomplete_edge{Ta, Ts}(edge::MeanEdge{Ts, Ta}, startIndex::Array{Int32, 1})
    insert_incomplete_edges([edge], startIndex)
end

end
