# seff-array

An extension of the Slurm command 'seff' designed to handle job arrays and offers the option to display information in a histogram.             
seff-array generates three types of histograms: 
    1. Maximum memory usage of each job in the array
    2. Runtime of each job
    3. CPU Utilization relative to the runtime

Usage: `seff-array [-h] [-m] [-t] [-c] [-v] jobid`
   `-m, --mem      show memory usage stats`
   `-t, --time     show time usage stats`
   `-c, --cpu      show cpu usage stats`
   `-v, --verbose  enable to show histograms for job arrays`

To use seff-array on the job array with ID `12345678`, simply run `seff-array 12345678`.
The default when no flags given is -mtc (all stats). 
Without the -v flag, seff-array produces output similar to the output of seff for both single jobs and job arrays.


Output Style:
Histograms and headings are designed to fit to 80% of your terminal window size. 
Note: if your terminal window is too small (less than ~45 columns wide), then `seff-array` assumes
a terminal window of 100 columns wide. 

Histogram bins vary according to the distribution of the data.There is a minimum bin width associated with each 
type of histogram to avoid unnecessary precision when the data is extremely clustered. 
