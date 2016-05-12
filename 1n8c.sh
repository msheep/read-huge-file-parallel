#!/bin/bash
#SBATCH --time=01:00:00
#SBATCH --mem-per-cpu=7G
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task=1
#SBATCH --job-name=1n8c
#SBATCH --error=1n8c-mpi_file-%j.ext2
#SBATCH --output=1n8c-mpi_file.%J.out

module load Python/3.4.3-goolf-2015a
module load OpenMPI/1.10.2-GCC-4.9.2
mpirun python mpi_file.py melbourne
