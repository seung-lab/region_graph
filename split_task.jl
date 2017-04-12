edges = Set{Tuple{Int,Int}}()
for fn in filter(x->ismatch(r"incomplete_edges_\d+_\d+_\d+.txt",x), readdir("."))
    open(fn) do f
        for ln in eachline(f)
            seg1, seg2 = [parse(Int,x) for x in split(ln, " ")]
            p = minmax(seg1, seg2)
            push!(edges, p)
        end
    end
    println("$(length(edges)) edges to process")
end

nprocess = 8
chunksize = div(length(edges), nprocess)+1
count = 0
idx = 0
f = 0
for p in edges
    if count == 0
        f = open("input_$idx.txt", "w")
    end
    write(f, "$(p[1]) $(p[2])\n")
    count += 1
    if count >= chunksize
        close(f)
        idx += 1
        count = 0
    end
end
