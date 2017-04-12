function assign_zero(pos, seg, window_size)
    (xdim,ydim,zdim)=size(seg)
    (xpos,ypos,zpos) = pos
    (xlow, ylow,zlow) = (max(xpos-window_size,1), max(ypos-window_size,1), max(zpos-window_size,1))
    (xup,yup,zup) = (min(xdim,xpos+window_size), min(ydim,ypos+window_size), min(zdim,zpos+window_size))
    min_distance = 100000
    segid = 0
    for z = zlow:zup
        for y = ylow:yup
            for x = xlow:xup
                if seg[x,y,z] != 0
                    distance = (abs(xpos-x) + abs(ypos-y) + abs(zpos-z))
                    if distance < min_distance
                        min_distance = distance
                        segid = seg[x,y,z]
                    end
                end
            end
        end
    end
    return segid
end

function expand_segments{Ts}(seg::Array{Ts,3})
    (xdim,ydim,zdim)=size(seg)
    d_size = 0
    new_seg = zeros(UInt32, (xdim, ydim, zdim))
    d_segid = Set()
    for z=1:zdim
      #println("working on z=$z")
      for y=1:ydim
        for x=1:xdim
            if seg[x,y,z] == 0
                window_size = 1
                d_size += 1
                segid = 0
                while window_size < xdim
                    segid = assign_zero((x,y,z), seg, window_size)
                    if segid == 0
                        window_size *= 2
                    else
                        break
                    end
                end
                new_seg[x,y,z] = segid
            else
                push!(d_segid, seg[x,y,z])
                new_seg[x,y,z] = seg[x,y,z]
            end
        end
      end
    end
    #println("number of segments: $(length(d_segid))")
    return new_seg
end
