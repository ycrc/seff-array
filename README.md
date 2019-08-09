# seff-array

An extension of the Slurm command 'seff' designed to handle job arrays and display information in a histogram.             
seff-array generates two histograms; one for the maximum memory usage of each job in the array, and one for the runtime of each job.

To use seff-array on the job array with ID '12345678', simply run 'seff-array 12345678'.
You can also use seff-array on a text file containing the results of the command:

'sacct -p -j <your_id> --format=JobID,JobName,MaxRSS,Elapsed,ReqMem,ReqCPUS,Timelimit'

To do this, use the '-i' flag: 'seff-array -i <filename.txt>'
