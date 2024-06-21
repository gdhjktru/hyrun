# hyrun

Tools for running calculations using the Hylleraas Software Platform


# jobs

central object are jobs, which have the following structure:

jobs = {0: {'job': hyrun.Job,
            'scheduler': hyrun.Scheduler,
            'database', hydb.Database},
             'progress_bar': tqdm.tqdm, ...}

where hyrun.Job is json serializable, i.e. can be stored in the database.
It contains attributes that inform about the job, e.g. id (slurm-id),
db_id (database id), scheduler (string), database (string),
tasks (list of hyset.RunSettings), outputs (list of hyrun.job.Output
corresponding to the tasks) etc.

The instances of scheduler, database etc, that is non-serializable objects
are thus stored in the other keys of jobs[<job_no>].

The form as a dict allows for easier picking out specific jobs, e.g. for
bundling those with same scheduler.

jobs are generated from
* list of run settings
* list of ints (database-ids), together with a kw database=...
* list of Jobs
using gen_jobs()

For local jobs, len(jobs[<job_no>]['job'].tasks) is always 1., for remote jobs,
e.g.slurm a length > 1 means that jobs are bundled within one job_script.
That is an Input [[RS0, RS1], [RS2, RS3]] (RS==RunSettings
 e.g. result of hyif.Interface.setup()) with scheduler=local will result in
 4 consecutive calculations, and for scheduler=slurm will create 2 jobs where
 each job_script is performing 2 calculations consecutively.

 The decocator hyrun.job.loop_over_jobs is used to convert routines for single
 jobs into thos that can perform tasks for all jobs. These routines are thus
 called with routine(jobs, *args, **kwargs) and using the decorator converted
 into a list of calls with the kwargs jobs[<job_no>].values().

# running jobs

hyrun.run(<input>) is performing the following steps:

1. create jobs using gen_jobs()
2. checking for finished jobs, marked with jobs[<job_no>]['job'].finished: bool
i.e. if the output files can be found locally and force_recompute is False
[Note: currently, remotely parsed jobs are not regarded by this]
3. finished jobs are removed
4. jobs are prepared in a loop over jobs (see abve):
    a.) job_script (hyset.File) is created for both remote and local jobs




5. jobs are added to database, if db_id is set, database entries are updated
6. check if dry run is set in all tasks in all jobs, exiting if True
