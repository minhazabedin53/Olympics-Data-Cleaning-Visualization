# This program runs your application and times it
# We assume that your application has a main() function
# which accepts no arguments and forms the starting point
# for your application.  
# While you can alter this file if you wish for testing purposes,
# the un-altered original form of this file will always be used
# for actual submission testing.  Thus, any changes made to this
# file will not be part of your application.  No features 
# should be implemented in this file as it will not be used during
# submission testing.

# To use this runner:
# Implement your application with a starting point of main() in project.py
# Run the following command:
#     python runproject.py


from project import main
import time
start_time = time.perf_counter()
main()
end_time = time.perf_counter()
total_time = (end_time-start_time)
print(f"EXECUTION_TIME: {total_time:.3f}")
