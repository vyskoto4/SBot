# SBot

Running the pipeline
--------------------

To run the Hadoop jobs pipeline, run the following command (check if conf.xml contains your desired configuration beforehand) :
```
hadoop jar SBotMiner-1.0.jar  com.ccc.SBotMiner.Pipeline.PipelineRunner -conf conf.xml
```
Focusness Groups Evaluation
----------
To evaluate the focusness groups, you need the following files:
* "User stats file" - the result of UserStats job
* "Query dictionary file" - one of the results of UserQuery job( removing repeated lines  is recommended)
* "Focusness groups file" - one of the results of MatrixBuilder job

Then just supply the results_stats python script with these files according to the first block comment section.

Focusness Groups Evaluation
----------
To evaluate the focusness groups, you need the following files:
* "User stats file" - the result of UserStats job
* "Query dictionary file" - one of the results of UserQuery job (removing the repeated lines is recommended)
* "All groups file" - one of the results of MatrixBuilder job

Then run the pcas.py script's run_svds function with the "All groups file" as source file. Note the new "bot_groupstats" file will be one of the inputs of results_stats script.
Finally just supply the results_stats python script with these files according to the first block comment section.

