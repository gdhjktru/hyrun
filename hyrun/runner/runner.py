from hyrun.scheduler import get_scheduler as _get_scheduler

class Runner:

    def __init__(self, *args, **kwargs):

        self.run_settings = self.get_run_settings(*args)
        self.scheduler = self.get_scheduler(**kwargs)
        
    def get_run_settings(self, *args):
        
        if len(args) > 1:
            raise ValueError("run() takes at most 1 positional argument, " +
                                "got {}".format(len(args)))
        return args[0]
    
    def get_scheduler(self, **kwargs):
        self.scheduler = getattr(self.run_settings, 'scheduler', None)
        if self.scheduler is None:
            raise ValueError("Scheduler not specified in run_settings")
        return _get_scheduler(self.scheduler, **kwargs)
    

    def run():
        # Run the context
        with self.scheduler.run_ctx():
            # Copy/send files
            if ...:
                ...
            
            # Generate input
            self.scheduler.generate_input(s)
            
            # Submit the job
            self.scheduler.submit()
            
        # Wait for the job to finish
        while status < timeout:
            self.scheduler.get_status()
            
        # Fetch the results
        self.scheduler.fetch_results()
        
        # Teardown the scheduler
        self.scheduler.teardown()

