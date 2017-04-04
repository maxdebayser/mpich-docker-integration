local OptionParser = require"std.optparse"
local lpeg = require"lpeg"
local posix = require"posix"

function verifyUndeclared(_, n)
    if (n ~= "_PROMPT") then
        error("attempt to read undeclared variable " .. n)
    end
end

setmetatable(_G, {
    __index = verifyUndeclared,
})

command = {
    short = "Run MPI in docker containers",
    long = [[
IBM Research 2017

Usage: dockerw -f hosts -n procs img cmd

Run a command with mpi exec in a cluster of ephemerous containers

dockerw will start one worker container for every host in the host file
if the number of processes is equal or greater than the number of hosts.
Otherwise the number of containers started will be the same as the number of
processes

Options:

      -n, --procs=N         The total number of processes
      -f, --hosts=hostfile  A file containing all available hosts as well as the number of processors
          --version         display version information, then exit
          --help            display this help, then exit

Please report bugs to mbayser@br.ibm.com
]],
    validate = function(argErr, args, opts)
        if #args < 2 then
            argErr("You must specify the image name followed by a command for mpiexec to run")
        end
        local config = {}

        if opts.procs then
            config.procs = tonumber(opts.procs)
        end
        if opts.hosts then
            config.hostfile = opts.hosts
            config.hostspec = parseHostFile(config.hostfile)
        end
        config.image = args[1]
        config.command = deepCopy(args)
        table.remove(config.command, 1)
        return config
    end,
    execute = function(config)

        local configFile, err = loadfile("config.lua")
        if not configFile then
             error("Error loading file config.lua: "..err)
        end
        configFile()

        run(config)
    end
}


function deepCopy(el)
    if type(el) ~= "table" then
        return el
    else
        local copy = {}
        for k,v in pairs(el) do copy[deepCopy(k)] = deepCopy(v) end
        return copy
    end
end

 -- Begin lpeg error handling wrapper
lpeg.locale(lpeg)

local P, S, V = lpeg.P, lpeg.S, lpeg.V
local C, Carg, Cb, Cc = lpeg.C, lpeg.Carg, lpeg.Cb, lpeg.Cc
local Cf, Cg, Cmt, Cp, Ct = lpeg.Cf, lpeg.Cg, lpeg.Cmt, lpeg.Cp, lpeg.Ct
local alpha, digit, alnum = lpeg.alpha, lpeg.digit, lpeg.alnum
local xdigit = lpeg.xdigit
local space = lpeg.space

function escape(str)
    local s = str
    s = string.gsub(s, "\n", "\\n")
    s = string.gsub(s, "\r", "\\r")
    return s
end

local function loc (str, where)
    local line, pos, linepos = 1, 1, 1
    local lbegin, lend = 1
    while true do
        pos = string.find (str, "\n", pos, true)
        if pos and pos < where then
            line = line + 1
            linepos = pos
            pos = pos + 1
        else
            lbegin = linepos
            lend = pos
            break
        end
    end
    return line, (where - linepos), lbegin, lend
end


function newLabeledFailure(str, pos, label, msg)
    return {str = str, pos = pos, label = label, msg = msg}
end

function newState()
    return {
        currentLabel = nil,
    }
end


function fail(msg, l)
    return Cmt(Carg(1), function(s, i, state)
        state.currentLabel = newLabeledFailure(s, i, l, msg)
        return false
    end) * P(false)
end

function Pf(pat, l)
    l = l or pat
    return P(pat) + fail(fmt("expected %s", escape(tostring(pat))), l)
end

function clear()
    return Cmt(Carg(1), function(s, i, state)
        state.currentLabel = nil
        return i
    end)
end

function getLabel(state)
    if state.currentLabel then
        return state.currentLabel.label
    end
end

function find(t,e)
    if not t then
        return not e
    end
    for _,v in ipairs(t) do if v == e then return true end end
    return false
end

function Y(p1, p2, ls)
    return p1*clear() + (
        Cmt(Carg(1), function(s, i, state)
           local l = getLabel(state)
           if not find(ls, l) then
               return nil
           else
               return i
           end
        end)
        * p2
    ) + P(false)
end

function trace(num)
    return Cmt(P(true), function(s, i)
        print("checkpoint "..tostring(num))
        return i
    end)
end

newline   = P("\r\n") + S("\r\n"); -- cr + lf
wspace    = S(" \t")
xwspace   = S(" \t\r\n")
Hex       = ((P("0x") + P("0X")) * xdigit^1) + fail("Expected a hexadecimal integer", "HEX")
Expo      = S("eE") * S("+-")^-1 * digit^1;
Float     = ((((digit^1 * P(".") * digit^0) + (P(".") * digit^1)) * Expo^-1) + (digit^1 * Expo)) + fail("Expected a floating point number", "FLOAT")
Int       = digit^1 + fail("Expected an integer", "INT")

Number    = ((P"-"^-1 * (Hex + Float + Int)) / function (n) return tonumber(n) end) + fail("Expected a number", "Number")
Comment   = C(P("--") * (1 - P("\n"))^0)

function lmatch(pat, str, pos)
    local state = newState()
    local ret = { lpeg.match(pat, str, pos or 1, state) }
    if not ret[1] then
        if state.currentLabel then
            local c = state.currentLabel
            local line, col, lbegin, lend = loc(c.str, c.pos)
            if lend then lend = lend - 1 end
            local msg = string.format("Error at line %d, column %d: %s\nContext:\n%s\n", line, col, c.msg, string.sub(c.str, lbegin, lend))
            local s = '^'
            for k=2,col do s = '~'..s end
            msg = msg..s
            return nil, msg
        else
            return nil, "Unknown error"
        end
    end
    return unpack(ret)
end

function multiply_pattern(prefix, item)
    return lpeg.Cmt(Carg(1)*prefix*Ct(item^0), function(s,i,state,count,t)
        if #t ~= count then
            state.currentLabel = newLabeledFailure(s, i, "MULT", fmt("A set of exactly %d elements was expected, %d were found", count, #t))
            return nil
        else
            return i, t
        end
    end)
end


function multiply_pattern_numeric(prefix, item)
    local t = {}

    local count = nil
    local pos = 0
    local function peek_count(s, i, c)
        count = tonumber(c)
        if ffi then
            t = new_float_array(count)
        end
        return i
    end

    local function insert_number(s,i,n)
        pos = pos + 1
        if pos <= count then
            t[pos] = tonumber(n)
        end
        return i
    end

    return lpeg.Cmt(Carg(1)*lpeg.Cmt(prefix,peek_count)*lpeg.Cmt(C(item), insert_number)^0, function(s,i,state, ...)
        local rest = {...}
        print("size of captures = ", #rest)
        if pos ~= count then
            state.currentLabel = newLabeledFailure(s, i, "MULT", fmt("A set of exactly %d elements was expected, %d were found", count, pos))
            return nil
        else
            return i, t
        end
    end)
end


function anywhere (p, ...)
    local labels = {...}
    return lpeg.P{ Y(p, 1 * P(V(1)), labels) }
end

-- End lpeg error handling wrapper

function parseHostString(hosts)
    local hostString_G = {
         "hosts",
         hostname = C((alnum + S("-_"))^1);
         procs    = C(Int^1);
         host     = Ct((Cg(V"hostname", "hostname") * ":" * Cg(V"procs", "procs")) + Cg(V"hostname", "hostname"));
         hosts    = Ct((wspace^0 * V"host" * wspace^0 * newline^1)^1);
    }
    return assert(lmatch(hostString_G, hosts))
end

function parseHostFile(filePath)
    local hostFile = assert(io.open(filePath))
    local input = hostFile:read"*all"
    hostFile:close()
    return parseHostString(input)
end

function planExecution(config)

    local exec_hosts = {}

    local nprocs = config.procs or 1

    if config.hostspec then
       local alloc_guard = nprocs

       for _,v in ipairs(config.hostspec) do
          slots = v.procs or 1
          table.insert(exec_hosts, {v.hostname, slots})
          alloc_guard = alloc_guard - slots
          if alloc_guard <= 0 then break end
       end

    else 
       for i=1,nprocs do
           --we repeat localhost to simplify
           table.insert(exec_hosts, {"localhost", 1})
       end 
    end
    return exec_hosts
end


function runDockerCommand(contname, img, cmd, host)
     local cmd_t = {"/usr/bin/docker", "-H", options.docker_host, "run", "-i", "--rm", "--net=host", "--name", contname }

     for _,opt in ipairs(options.extra_docker_options or {}) do table.insert(cmd_t, opt) end
     table.insert(cmd_t, img)

     if type(cmd) == "table" then
         for k,v in ipairs(cmd) do table.insert(cmd_t, v) end
     elseif type(cmd) == "string" then
         table.insert(cmd_t, cmd)
     else
         print("wrong cmd type")
         return nil
     end

     if host then
         table.insert(cmd_t, 1, host)
         table.insert(cmd_t, 1, "/usr/bin/ssh")
     end
     local cmd = table.concat(cmd_t, " ")
     print("going to run", cmd)
     return io.popen(cmd)
end


function runDockerCommand_swarm(contname, img, cmd, host_constraint)
     local cmd_t = {"/usr/bin/docker", "-H", options.docker_host, "run", "-i", "--rm", "--net=mpi_exp2", "--name", contname, "--hostname", contname, host_constraint }

     for _,opt in ipairs(options.extra_docker_options or {}) do table.insert(cmd_t, opt) end
     table.insert(cmd_t, img)

     if type(cmd) == "table" then
         for k,v in ipairs(cmd) do table.insert(cmd_t, v) end
     elseif type(cmd) == "string" then
         table.insert(cmd_t, cmd)
     else
         print("wrong cmd type")
         return nil
     end

     local cmd = table.concat(cmd_t, " ")
     print("going to run", cmd)
     return io.popen(cmd)
end



function run(config)
    local hosts = planExecution(config)

    local hoststring = {}

    if options.mode == 'single' then
        for k,v in ipairs(hosts) do
            local contname = "worker_"..tostring(k)
            if #hoststring > 0 then table.insert(hoststring,",") end 
            table.insert(hoststring, contname)
            table.insert(hoststring, ":")
            table.insert(hoststring, tostring(v[2]))
        end
    else
        for k,v in ipairs(hosts) do
            if #hoststring > 0 then table.insert(hoststring,",") end 
            table.insert(hoststring, v[1])
            table.insert(hoststring, ":")
            table.insert(hoststring, tostring(v[2]))
        end
    end
    hoststring = table.concat(hoststring)

    local full_command = { "/usr/lib64/mvapich2/bin/mpiexec", "--launcher", "manual", "-n", tostring(config.procs or 1), "-hosts", hoststring }

    for _,v in ipairs(config.command) do
        table.insert(full_command, v)
    end

    local mpiexec_proc =  nil
    if options.mode == 'single' then
        mpiexec_proc = runDockerCommand_swarm("master", config.image, full_command, "")
    else
        mpiexec_proc = runDockerCommand("master", config.image, full_command)
    end

    local hydra_commands = {}

    while true do
        local line = mpiexec_proc:read("*line")
        if not line then break end
        if line == "HYDRA_LAUNCH_END" then break end

        local hydra_cmd, subs = string.gsub(line, "HYDRA_LAUNCH: ", "")
        if not subs then
            print("unrecognized line from mpiexec:")
            print("line")
            os.exit(1)
        end
        if options.mode == 'single' then
            hydra_cmd = string.gsub(hydra_cmd, "masternode", "master")
        end
        table.insert(hydra_commands, hydra_cmd)
    end

    local worker_procs = {}

    for i=1,#hydra_commands do
        local contname = "worker_"..tostring(i)
        if options.mode == 'single' then
            table.insert(worker_procs, runDockerCommand_swarm(contname, config.image, hydra_commands[i], "-e constraint:node==nx36000"..tostring(i)..".compute"))
        else
            table.insert(worker_procs, runDockerCommand(contname, config.image, hydra_commands[i], "nx36000"..tostring(i)..".compute"))
        end
    end

    while true do
        local line = mpiexec_proc:read("*line")
        if not line then break end
        print(line)
    end

    mpiexec_proc:close()

end


local parser = OptionParser(command.long)
args, opts = parser:parse (arg)
local config = command.validate(function(...) parser:opterr(...) end, args, opts)
command.execute(config)



