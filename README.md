# seff-array 

An extension of the Slurm command 'seff' designed to handle job arrays and offers the option to display information in a histogram.       

seff-array generates three types of histograms: 

    1. CPU Efficiency (utilization vs runtime)
    1. Maximum memory usage versus the requested memory
    2. Runtime of each job compared to the requested wall-time

## Usage:

    seff-array [-h] jobid

To use seff-array on the job array with ID `12345678`, simply run `seff-array 12345678`.
For job-arrays, statistics and histograms will be produced for CPU, memory, and time efficiencies.
For single jobs, an output similar to `seff` will be produced.
