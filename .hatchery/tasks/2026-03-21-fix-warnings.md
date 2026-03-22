# Task: fix-warnings

**Status**: in-progress
**Branch**: hatchery/fix-warnings
**Created**: 2026-03-21 21:57

## Objective

We have the following test warnings, can you address them all?

tests/test_iracing_data.py::TestRaceResults::test_race_result
tests/test_real_integration.py::TestFullIntegration::test_single_season
tests/test_real_integration.py::TestFullIntegration::test_multi_season
  /Users/grado/Code/projects/gpt-racing/src/gpt_racing/iracing_data.py:62: DeprecationWarning: Returning raw dictionaries is deprecated and will be removed in a future version. Set use_pydantic=True to use Pydantic models for improved type safety. See documentation for migration guide.
    ir_client = irDataClient(access_token=_get_token())

tests/test_real_integration.py: 24 warnings
tests/test_render_tables.py: 2 warnings
  /Users/grado/Code/projects/gpt-racing/src/gpt_racing/render_tables.py:96: GuessedAtParserWarning: No parser was explicitly specified, so I'm using the best available HTML parser for this system ("html.parser"). This usually isn't a problem, but if you run this code on another system, or in a different virtual environment, it may use a different parser and behave differently.
  
  The code that caused this warning is on line 96 of the file /Users/grado/Code/projects/gpt-racing/src/gpt_racing/render_tables.py. To get rid of this warning, pass the additional argument 'features="html.parser"' to the BeautifulSoup constructor.
  
    soup = BeautifulSoup(html)

-- Docs: https://docs.pytest.org/en/stable/how-to/capture-warnings.html

## Agreed Plan

*(To be filled in after planning discussion)*

## Progress Log

*(Steps will appear here once the plan is agreed)*

## Summary

*(Fill in on completion — then remove Agreed Plan and Progress Log above.
Cover: key decisions made, patterns established, files changed, gotchas,
and anything a future agent working in this repo should know.)*
