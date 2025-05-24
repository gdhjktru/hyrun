hyrun
*****

.. note::

   **Nomenclature**

   - **workflow**: set of jobs with optional dependencies, represented by ``hyrun.job.JobGraph`` class
   - **job**: set of ordered tasks that can be run using a script, e.g. on a cluster, represented by ``hyrun.job.Job`` class
   - **task**: a single calculation or more general procedure that can be represented by ``hyset.RunSettings``
   - **result**: set of ordered outputs from a job, represented by a list.
   - **output**: outputs from a job, represented by ``hyrun.result.Result`` class.

The ``hyrun`` module offers the following methods:

1. ``get_workflow()``: creating a workflow
2. ``run()``: running a workflow
3. ``check()``: checking on a workflow, updates status of all jobs
4. ``fetch()``: checking on a workflow and fetch results of all finished jobs
5. ``stop()``: stops a workflow, cancels all submitted jobs
6. ``remove()``: removes all jobs of a workflow from the respective databases

All methods accept as input a (path to a) workflow, a (list of) job(s),
a (list of) task(s), a (list of) result(s), a (list of) output(s) or combination of that.
As a first step, a workflow will be created.