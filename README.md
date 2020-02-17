# bigfile-exc
This project is solving the Backend dev task for bigfile name matching
The project enables to define:
- a list of names (default is 50 most common en)
- Textfile to process (any size)
- Chunk-size (default 80K - appoximate 1000 lines of text)
Running it will print per word the list of offsets (line & char).

## Installation
- make sure you have Python3
- Clone from git
- run install.sh

## Running the project
- can be opened in IDE and run tests/main_test.py (need to make bigfile-exc a source root)
- go to tests dir and run "Python3 main_test.py"
- if you want to change the defaults, edit main_test.py

## Assumptions
1. A token is of the regexp [A-Za-z0-9_]+ (this is the case in the example, if this changes, it may require a more advanced parser)
2. matching will be done by the lowercase of a full token as defined in #1 this means:
- David matches david, DaVid but does not make a match with David1 DavidJohn David_
- David will be matched in lines containing David?John, David-John
- *Reasoning*: in the sample file I already saw that names are lowercase, Capitalized or UPPERCASE. In order to also cover case typos would make sense to use this method.
3. Runs on one machine but the file-size is not limited. it is assumed that the result can fit in memory.
4. No need to print words that did not appear in the file.

## Some Implementation details
Overall there are 3 main modules: Manager, Matcher and Aggregator, plus data modules for the word and chunk matching data
- The manager is responsible for chunking the file, putting them in a queue for the matching workers and waiting if the queue is full (currently limiting all waiting chunks to no more than 1GB)
- The Matcher runs with a designated number of workers (defaults to 10) that are getting their chunks in a Queue. They will calculate all matches in the chunk level and pass to the aggregator.
- The matcher is using the ntlk regexp parser using the above assumptions for word tokenizing. it is important to avoid "finding" names that are part of a bigger word.
- The Aggregator is also waiting on a Queue and will aggregate all the matching data into one list.
- The aggregator calculates internally the line offsets per chunk and appends to the output when printing
- Synchronizing the exit of the workers and printing through the queues
- All classes are encapsulated and communicate through function calls (sometimes with queues inside for asynch)
