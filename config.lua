lposix=require'posix'


local uid = lposix.unistd.getuid()
local pw_entry = lposix.pwd.getpwuid(uid)
local username= pw_entry.pw_name
local home_dir = pw_entry.pw_dir


options = {
  --mode = 'cluster';
  --docker_host = 'unix:///var/run/docker.sock';
  mode = 'single';
  docker_host = ':4000';
  extra_docker_options = {
    "-v", home_dir..":"..home_dir,
    "--ulimit memlock=16777216", "--device=/dev/infiniband/uverbs0:/dev/infiniband/uverbs0", 
  }
}
