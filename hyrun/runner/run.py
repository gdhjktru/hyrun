
from runner.runner import Runner


# Runner:

# Scheduler = ...

# With scheduler.run_ctx: (can be a connection opening)
# 	if ...copy/send files
# 	scheduler.generate_input(s) (script/cmd)
# 	-> db (maybe exit)
# 	scheduler.submit
# 	-> db (maybe exit)
# while status<timeout
# 	scheduler.get_status
# 	-> db (maybe exit)
	
# scheduler.fetch_results
# scheduler.teardown
# parse results
# compute_settings.further_teardown

# REVERSE ORDER TEARODWN/PARSE


def run(*args, **kwargs):
    return Runner(*args, **kwargs).run()

   
async def arun(*args, **kwargs):
    return await Runner(*args, **kwargs).arun()