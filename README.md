# hyrun

Tools for running calculations using the Hylleraas Software Platform

## usage

hyrun.run() will replace hyset.Runner.run() and can thus be used like this:
hyif.Interface.parse(hyrun.run(hyif.Interface.setup(object))).
However, in addition to RunSettings (==hyif.Interface.setup(object)), it can
also take a database id, a Job object and, inprinciple combinations of it.
hyrun.run() will thus return either 2d-array of hyrun.Output() objects (default), list of
 integers (database ids, for wait=False) or hyrun.Job() (for dry_run=True).

 A new feature is automatic checking for jobs: if hyrun.run() is called twice
 with the same job information, hyrun.run() will check before submission if the job hash
 (constructed from the job-script, and thus also from the input-hash for many interfaces)
 exists in the database and instead check the status of this job and eventually
 fetch the corresponding results instead of running the job twice.
 This behaviour can be switch off by setting in the ComputeSettings rerun=True.

## jobs

Central object of hyrun are jobs, which have the following structure:

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

 The decocator hyrun.job.loop_over_jobs is used to convert routines for single
 jobs into thos that can perform tasks for all jobs. These routines are thus
 called with routine(jobs, *args, **kwargs) and using the decorator converted
 into a list of calls with the kwargs jobs[<job_no>].values().

## generating jobs

jobs are generated using hyrun.runner.gen_jobs() from
* (list of) run settings
* (list of) ints (database-ids), together with a kw database=...
* (list of) Jobs

For local jobs, len(jobs[<job_no>]['job'].tasks) is always 1., for remote jobs,
e.g.slurm a length > 1 means that jobs are bundled within one job_script.
That is an Input [[RS0, RS1], [RS2, RS3]] (RS==RunSettings
 e.g. result of hyif.Interface.setup()) with scheduler=local will result in
 4 consecutive calculations, and for scheduler=slurm will create 2 jobs where
 each job_script is performing 2 calculations consecutively.

 gen_jobs() is making sure that all tasks in a job have the same database
 and scheduler and initialize them.

 Furthermore, for scheduler=local it will make sure that there is only 1
 task per job and thus add jobs if applicable. For slurm jobs, it is checked
 that the ressources asked for in the job script, e.g. mermory_per_cpu, ntasks,
 etc. and submit_dir_remote are identical for all tasks in on job.


## running jobs

hyrun.run(<input>) is performing the following steps:

1. create jobs using gen_jobs()
2. checking for finished jobs, marked with jobs[<job_no>]['job'].finished: bool
i.e. if the output files can be found locally and force_recompute is False
[Note: currently, remotely parsed jobs are not regarded by this]
3. finished jobs are removed from jobs dict

4. a list of the different schedulers is created (two scheduler are equal if also all the dicts self.connection (with host and user etc) match)
5. check wether to wait (set if at least one task of all jobs has set wait=True)
6. if wait=True and more than one scheduler is found, write a warning, because currently, all jobs of schedulers[i] are submited and waited for before all jobs of schedulers[i] are considered
7. loop over schedulers start
8. create scheduler context (nullcontext for local scheduler, ssh-conneciton for slurm scheduler)
9. get jobs with correct scheduler

10. jobs are prepared in a loop over jobs (see abve):
    a.) job_script (hyset.File) is created for both remote and local jobs and written to disk (submit_dir_local)
    b.) job hash is created from job script content
    c.) all files in files_to_write are written to disk
    d.) all file objects in RunSettings are 'resolved', i.e. converted in to a dict with keys
    'path' and 'host' File(...) -> {'path': ..., 'host': ...}
    e.) all objects in Job() are checked for compatibility with hydb.

11. jobs are added to database, if db_id is set, database entries are updated, if job.hash is found in database, job is fetched form database
12. check if dry_run is set in at least one tasks of jobs, exiting if True returning a dict {'<databasename>':['db_id1', ....]}
13. if rerun is set True in at least one tasks of jobs, those jobs that have identical hash to a job in the database, are fetched from the database and, if completed the files are transferred back. All other jobs continue the run

14. collect all files from all active jobs (of given scheduler) and send them to cluster. Note: they are all send to the SAME directory, submit_dir_remote, e.g. /cluster/work/users/${user} which might thus clog up easily
15. submit jobs from submit_dir_remote, and jobs[<job_no>].id and jobs[<job_no>].outputs[x].output_file, etc are updated
16. update database. if scheduler=local, the calculation is finished, stdout_err and stdout_file are written to work_dir_local and jobs[<job_no>]['job'].outputs[0] is updated and jobs[<job_no>]['job'].status is set to 'COMPLETED' or 'FAILED' depending on the returncode. For scheduler=slurm, the scheduler context is used to submit the job script and the jobs[<job_no>].job_id is set to the slurm id and jobs[<job_no>].status is set to 'SUBMITTED'.
17. if wait is False, the scheduler loop is continued (goto 10) and eventually, the database ids are returned as a dict {'<databasename>':['db_id1', ....]}. If wait is True, the maximum waiting time is max(sum(task.job_time for task in job.tasks) for job in jobs)
18. after waiting jobs in database are updated with the latest status
19. files are transferred back from the cluster, if transfer_all is set (default) all files from the remote working directory, e.g. /cluster/work/users/${user}/${job_id} are downloaded, if not, only output_file, stderr_file and stdout_file are transferred
20. the scheduler is teared down
21. the outputs are returned, these can be parsed by hyif.Interface.parse()


## checking on jobs

hyrun.get_status() is similar to run with steps 1.-7. then the current status is fetched from the scheduler and returned as a string.
if Fetch results=True, the results are copied back and the outputs are returned.


## fetching results

hyrun.fetch_results() is calling hyrun.get_status() with fetch_results=True
