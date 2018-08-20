Dear CRAN maintainers,

This release patches the issue in which a unit-test attempted to use a Suggested
package without first checking that said package was installed.  This also corrects
a grammatical error in the package DESCRIPTION.  


## Test environments
* local OS X install, R 3.5.1
* ubuntu 14.04 (on travis-ci), R 3.5.1
* win-builder (devel and release)

## R CMD check results

0 errors | 0 warnings | 1 note

* This is a new release.
