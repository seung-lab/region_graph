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
for i in 0:53
for j in 0:35
    fn = "incomplete_edges_$(i)_$(j)_0.txt"
    pos = "$(i)_$(j)_0"
    open("$(ARGS[1])/$fn") do f
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
end

nprocess = 1024
ninstance = 1
idx = 0
process_instance(edges, idx)
