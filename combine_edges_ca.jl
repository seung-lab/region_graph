include("contact_area.jl")

function load_voxels(fn, edge)
    open(fn) do f
        while (!eof(f))
            i = read(f,Int64)
            coord = read(f,Int32,3)
            sem1 = read(f,Float32,4)
            sem2 = read(f,Float32,4)
            edge.boundaries[i][coord] = [sem1, sem2]
        end
    end
end

function write_edges{Ts}(fout, seg1::Ts, seg2::Ts, data)
    p = minmax(seg1, seg2)
    edge = ContactEdgeSem(Dict{Array{Int32,1}, SemanticInfo}[Dict{Array{Int32,1}, SemanticInfo}(),Dict{Array{Int32,1}, SemanticInfo}(), Dict{Array{Int32,1}, SemanticInfo}()])
    for s in data
        fn = "sem/$(seg1)_$(seg2)_$(s).txt"
        load_voxels(fn,edge)
    end
    results = process_edge(p, edge)
    for j in 1:length(results)
        count = results[j][1]
        vol = results[j][2]
        com = results[j][3]./vol
        bbox = results[j][4]
        sem1 = results[j][5]
        sem2 = results[j][6]
        write(fout, "$(p[1])_$(p[2])_$(j) $(count[1]) $(count[2]) $(count[3]) $(com[1]) $(com[2]) $(com[3]) $(bbox[1]) $(bbox[2]) $(bbox[3]) $(bbox[4]) $(bbox[5]) $(bbox[6]) $(sem1[1]) $(sem1[2]) $(sem1[3]) $(sem1[4]) $(sem2[1]) $(sem2[2]) $(sem2[3]) $(sem2[4])\n")
    end
end

open(ARGS[1]) do fin
open(ARGS[2],"w") do fout
    for ln in eachline(fin)
        data = split(strip(ln), " ")
        seg1, seg2 = [parse(Int32, x) for x in data[1:2]]
        write_edges(fout, seg1, seg2, data[3:end])
    end
end
end
