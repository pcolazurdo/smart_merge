# Some extra ideas:

* Create a test for the whole project
* try to avoid using with alive_bar() and move to a function like mechanism
* move the DO_* to a config class
* add help info on the arguments
* Create the option to calculate the size of the job before running 
* multi threaded
* create a class for session_id so we can keep track of all the sessions_id generated
* improve stats output - in particular, manage messages depending on flags
* add pause, and control+interrupts control
* think about alternative to session-id for renaming files ... woud be nice not to copy again if file exists even if user has decided not to use delete source
* create different output files depending on configuration:
    * script option
    * new file names option
    * stats output