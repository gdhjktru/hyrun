from hyrun.scheduler import get_scheduler as _get_scheduler

class Runner:
    """Runner."""

    def __init__(self, *args, **kwargs):
        """Initialize."""
        self.run_settings = self.get_run_settings(*args)
        self.scheduler = self.get_scheduler(**kwargs)
        
    def get_run_settings(self, *args):
        """Get run settings."""
        if len(args) > 1:
            raise ValueError("run() takes at most 1 positional argument, " +
                                "got {}".format(len(args)))
        return args[0]
    
    def get_scheduler(self, **kwargs):
        """Get scheduler."""
        self.scheduler = getattr(self.run_settings, 'scheduler', None)
        if self.scheduler is None:
            raise ValueError("Scheduler not specified in run_settings")
        return _get_scheduler(self.scheduler, **kwargs)
    

    # def wait(self, timeout=60):
    #     """Wait for job to finish."""
        
    #     while True:
    #         status = self.scheduler.get_status()
    #         if status >= timeout:
    #             break
    #         self.scheduler.get_status()
        
    #     timeout = self.run_settings.walltime or timeout
    #     status = self.scheduler.get_status()
    #     while status < timeout:
    #         self.scheduler.get_status()

    # def infinite_palindromes():
    # num = 0
    # while True:
    #     if is_palindrome(num):
    #         i = (yield num)
    #         if i is not None:
    #             num = i
    #     num += 1

    

    def run(self, s, timeout=60):
        # Run the context
        with self.scheduler.run_ctx():
            # Copy/send files
            
            # Generate input
            self.scheduler.generate_input(s)
            
            # Submit the job
            self.scheduler.submit()
            
        # Wait for the job to finish

        if not self.run_settings.wait:
            return
        
        timeout = self.run_settings.walltime or timeout
        status = self.scheduler.get_status()
        while status < timeout:
            self.scheduler.get_status()
            
        # Fetch the results
        self.scheduler.fetch_results()
        
        # Teardown the scheduler
        self.scheduler.teardown()

