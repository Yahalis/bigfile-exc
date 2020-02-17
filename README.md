# bigfile-exc
This project is solving the Backend dev task for bigfile name matching

## Assumptions
1. A word is of the structure [A-Za-z0-9_]+ (this is the case in the example, if this changes, it may require a more advanced parser)
2. matching will be done on a lowercase of a full word as defined in #2 this means:
- David matches david, DaVid but does not make a match with David1 DavidJohn David_
- David will be matched in lines containing David?John, David-John
- Reasoning: in the sample file I already saw that some
