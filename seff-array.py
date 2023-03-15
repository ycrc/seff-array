#!/gpfs/gibbs/pi/support/software/utilities/bin/python

import argparse
import subprocess
import sys

import numpy as np
import pandas as pd

from io import StringIO
import os

import termplotlib as tpl

__version__ = 0.4
debug = False


def time_to_float(time):
    """ converts [dd-[hh:]]mm:ss time to seconds """
    if isinstance(time, float):
        return time
    days, hours = 0, 0

    if "-" in time:
        days = int(time.split("-")[0]) * 86400
        time = time.split("-")[1]
    time = time.split(":")

    if len(time) > 2:
        hours = int(time[0]) * 3600

    mins = int(time[-2]) * 60
    secs = float(time[-1])

    return days + hours + mins + secs

#@profile
def job_eff(job_id=0, cluster=os.getenv('SLURM_CLUSTER_NAME')):

    if job_id==0:
        df_short = pd.read_csv('seff_test_oneline.csv', sep='|')
        df_long = pd.read_csv('seff_test.csv', sep='|')
    else:
        fmt = '--format=JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,NNodes,User,Group,Cluster'
        if cluster != None:
            q = f'sacct -X --units=G -P {fmt} -j {job_id} --cluster {cluster}'
        else:
            q = f'sacct -X --units=G -P {fmt} -j {job_id}'
        res = subprocess.check_output([q], shell=True)
        res = str(res, 'utf-8')
        df_short = pd.read_csv(StringIO(res), sep='|')

        fmt = '--format=JobID,JobName,Elapsed,ReqMem,ReqCPUS,Timelimit,State,TotalCPU,NNodes,User,Group,Cluster,MaxVMSize'
        if cluster != None:
            q = f'sacct --units=G -P {fmt} -j {job_id} --cluster {cluster}'
        else:
            q = f'sacct --units=G -P {fmt} -j {job_id}'
        res = subprocess.check_output([q], shell=True)
        res = str(res, 'utf-8')
        df_long = pd.read_csv(StringIO(res), sep='|')


    # filter out pending and running jobs
    finished_state = ['COMPLETED', 'FAILED', 'OUT_OF_MEMORY', 'TIMEOUT', 'PREEMPTEED']
    df_long_finished = df_long[df_long.State.isin(finished_state)]

    if len(df_long_finished) == 0:
        print(f"No jobs in {job_id} have completed.")
        return -1
        
    # cleaning
    df_short = df_short.fillna(0.)
    df_long  = df_long.fillna(0.)

    df_long['JobID'] = df_long.JobID.map(lambda x: x.split('.')[0])
    df_long['MaxVMSize'] = df_long.MaxVMSize.str.replace('G', '').astype('float')
    df_long['ReqMem'] = df_long.ReqMem.str.replace('G', '').astype('float')
    df_long['TotalCPU'] = df_long.TotalCPU.map(lambda x: time_to_float(x))
    df_long['Elapsed'] = df_long.Elapsed.map(lambda x: time_to_float(x))
    df_long['Timelimit'] = df_long.Timelimit.map(lambda x: time_to_float(x))

    # job info
    if isinstance(df_short['JobID'][0], np.int64):
        job_id = df_short['JobID'][0]
        array_job = False
    else:
        job_id = df_short['JobID'][0].split('_')[0]
        array_job = True
    
    job_name = df_short['JobName'][0]
    cluster = df_short['Cluster'][0]
    user = df_short['User'][0]
    group = df_short['Group'][0]
    nodes = df_short['NNodes'][0]
    cores = df_short['ReqCPUS'][0]
    req_mem = df_short['ReqMem'][0]
    req_time = df_short['Timelimit'][0]
    
    print("--------------------------------------------------------")
    print("Job Information")
    print(f"ID: {job_id}")
    print(f"Name: {job_name}")
    print(f"Cluster: {cluster}")
    print(f"User/Group: {user}/{group}")
    print(f"Requested CPUs: {cores} cores on {nodes} node(s)")
    print(f"Requested Memory: {req_mem}")
    print(f"Requested Time: {req_time}")
    print("--------------------------------------------------------")
    
    print("Job Status")
    states = np.unique(df_short['State'])
    for s in states:
        print(f"{s}: {len(df_short[df_short.State == s])}")
    print("--------------------------------------------------------")
    
    # filter out pending and running jobs
    finished_state = ['COMPLETED', 'FAILED', 'OUT_OF_MEMORY', 'TIMEOUT', 'PREEMPTEED']
    df_long_finished = df_long[df_long.State.isin(finished_state)]    

    if len(df_long_finished) == 0:
        print(f"No jobs in {job_id} have completed.")
        return -1
    
    cpu_use =  df_long_finished.TotalCPU.loc[df_long_finished.groupby('JobID')['TotalCPU'].idxmax()]
    time_use = df_long_finished.Elapsed.loc[df_long_finished.groupby('JobID')['Elapsed'].idxmax()]
    mem_use =  df_long_finished.MaxVMSize.loc[df_long_finished.groupby('JobID')['MaxVMSize'].idxmax()]
    cpu_eff = np.divide(np.divide(cpu_use.to_numpy(), time_use.to_numpy()),cores)

    print("--------------------------------------------------------")
    print("Finished Job Statistics")
    print("(excludes pending, running, and cancelled jobs)")
    print(f"Average CPU Efficiency {cpu_eff.mean()*100:.2f}%")
    print(f"Average Memory Usage {mem_use.mean():.2f}G")
    print(f"Average Run-time {time_use.mean():.2f}s")
    print("---------------------")
    
    if array_job:
        print('\nCPU Efficiency (%)\n---------------------')
        fig = tpl.figure()
        h, bin_edges = np.histogram(cpu_eff*100, bins=np.linspace(0,100,num=11))
        fig.hist(h, bin_edges, orientation='horizontal')
        fig.show()
        
        print('\nMemory Efficiency (%)\n---------------------')
        fig = tpl.figure()
        h, bin_edges = np.histogram(mem_use*100/float(req_mem[0:-1]), bins=np.linspace(0,100,num=11))
        fig.hist(h, bin_edges, orientation='horizontal')
        fig.show()
        
        print('\nTime Efficiency (%)\n---------------------')
        fig = tpl.figure()
        h, bin_edges = np.histogram(time_use*100/time_to_float(req_time), bins=np.linspace(0,100,num=11))
        fig.hist(h, bin_edges, orientation='horizontal')
        fig.show()

    print("--------------------------------------------------------")

if __name__ == "__main__":

    desc = (
        """
    seff-array v%s
    https://github.com/ycrc/seff-array
    ---------------
    An extension of the Slurm command 'seff' designed to handle job arrays and display information in a histogram.

    To use seff-array on the job array with ID '12345678', simply run 'seff-array 12345678'.

    Other things can go here in the future.
    -----------------
    """
        % __version__
    )

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=desc,
    )
    parser.add_argument("jobid")
    parser.add_argument("-c", "--cluster", action="store", dest="cluster")
    parser.add_argument('--version', action='version',  version='%(prog)s {version}'.format(version=__version__))
    args = parser.parse_args()

    job_eff(args.jobid, args.cluster)
