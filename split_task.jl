function process_instance(edges, idx)
    fragsize = div(length(edges), nprocess)+1
    count = 0
    idx_frag = 0
    f = 0
    mkdir("$idx")
    for p in keys(edges)
        if count == 0
            f = open("$(idx)/input_$(idx_frag).txt","w")
        end
        write(f,"$(p[1]) $(p[2]) $(join(edges[p], " "))\n")
        count += 1
        if count >= fragsize
            idx_frag += 1
            count = 0
            close(f)
        end
    end
end

edges = Dict{Tuple{Int,Int}, Array{String,1}}()
for fn in filter(x->ismatch(r"incomplete_edges_\d+_\d+_\d+.txt",x), readdir("./edges"))
    m = match(r"incomplete_edges_(\d+_\d+_\d+).txt", fn)
    pos = m.captures[1]
    open("edges/$fn") do f
        for ln in eachline(f)
            seg1, seg2 = [parse(Int,x) for x in split(ln, " ")]
            p = minmax(seg1, seg2)
            if !haskey(edges, p)
                edges[p] = String[]
            end
            push!(edges[p], pos)
        end
    end
    println("$(length(edges)) edges to process")
end

nprocess = 8
ninstance = 16
chunksize = div(length(edges), ninstance)+1
count = 0
idx = 0
edges_instance = Dict{Tuple{Int,Int}, Array{String,1}}()
for p in keys(edges)
    edges_instance[p] = edges[p]
    count += 1
    if count >= chunksize
        process_instance(edges_instance, idx)
        idx += 1
        count = 0
        edges_instance = Dict{Tuple{Int,Int}, Array{String,1}}()
    end
end
if length(edges_instance) > 0
    process_instance(edges_instance, idx)
end
