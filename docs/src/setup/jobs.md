hyrun
*****

> **Nomenclature**
> 
> - **workflow**: set of jobs with optional dependencies, represented by `hyrun.job.JobGraph` class  
> - **job**: set of ordered tasks that can be run using a script, e.g. on a cluster, represented by `hyrun.job.Job` class  
> - **task**: a single calculation or more general procedure that can be represented by `hyset.RunSettings`
> - **result**: set of ordered outputs from a job, represented by a list.
> - **output**: outputs from a job, represented by `hyrun.result.Result` class.

the `hyrun` module offers the following methods:

1. `init()`: creating a workflow
1. `run()` : running a workflow
2. `check()` : checking on a workflow
4. `fetch()` : fetch results from a workflow
3. `update()` : combines `check()` and `fetch()`
4. `stop()` : stops /  cancels a workflow

All methods accept as input a workflow, a (list of) job(s), a (list of) task(s),
a (list of) result(s), a (list of) output(s) or combination of that.
As a first step, a workflow will be created.


--



all take the same input:

JobGraph
path to JobGraph
runsettings
list of run_settings
result dict object
list of result objects
list of run_settings and result objects

kwargs: logger

steps:

1. if path, load graph
2. if graph, return it
3. else: generate graph of ( generate list of Jobs (generate hash))

4. save original order
5. take topological order
6. group order by number of ancestors
7. subgroup by hosts
loop over groups
loop over subgroups
combine data to be send
establish connection



