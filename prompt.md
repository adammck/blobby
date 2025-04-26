#codebase 

we're implementing the tombstones feature, the design for which can be found in `design.md`, using the steps which can be found in `implementation-steps.md`. these steps are detailed and have been pre-approved, so we should follow them closely and not improvise. if anything is unclear, stop and ask.

you can find the number of the next step to implement in `progress.md`. please:

1. implement this next step
2. tell me what you've done
3. update `progress.md` with the step number we will implement next
4. tell me what you expect the output of the relevant tests to be (and remember, it's okay if they fail between steps)
5. run the relevant tests
6. commit the change using the template below
7. stop.

we will continue iterating in this way until the feature is fully implemented.

commit messages should be in the format:

```
(agent) $feature$ ($step_we_just_implemented$/$total$): $very_brief_description$

$long_description$

Co-authored-by: $LLM_name$
```

and should follow the advice found at:
<https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html>
