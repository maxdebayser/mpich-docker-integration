# mpich-docker-integration
A docker orchestrator for MPI integration

This tool allow you to deploy a docker image to an HPC cluster and run the
entire simulation wrapped inside containers. You don't need to worry about
compiling your program and libraries on the cluster. Just deploy and run.

Out of the box, docker and MPI are difficult to integrate because most the
MPI launchers dominate the control flow from the beginning and make it
difficult to plug in alternative worker set up steps.

This tool uses the flexibility of the MPICH launcher to start all workers
in containers on behalf of the master process. It can be used on a single machine,
or in a cluster where it launches the workers with ssh or docker Swarm.
Any cloud platforms that support the standard docker API and support overlay
networks with name resolution are also supported.

Using device sharing, high speed networks like Infiniband are supported as well,
as long as all the necessary client libraries are installed in the docker image.

To run just execute:

  dockermpi -f myhosts mycontainer mycommand myargs
  
Where myhosts is a text file containing one entry with the format "hostname:num_cpus"
per line.
