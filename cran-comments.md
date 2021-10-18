Dear CRAN maintainers,

Changes in this release are described in NEWS.md.

This release (0.0.14) follows right behind the earlier release (0.0.13), 
because our previous release introduced new optional features that leverage
the arrow library, and these tests fail on Solaris. We apologize for not
catching this, but our tests did assert that they should only be run if 
arrow was available.  However, the arrow package is not properly installed 
and configured on solaris, as described in the error message that results:
as described in https://arrow.apache.org/docs/r/articles/install.html 

Until the CRAN Solaris machine is properly set up to use the arrow package,
we now gracefully skip the tests on that machine. 
 





Thanks!

Note that winbuilder will always throw a NOTE on this package due to the continued
use (as SUGGESTED only) of a CRAN package that is not in the standard repositories.  

winbuilder may also show a NOTE regarding a possible 304 ("Not Modified") code on
https://www.iana.org/assignments/media-types/text/tab-separated-values, the 
canonical IANA link defining this popular media-type format.  I do not believe there
is a preferable link here, but if the 304 code is problematic to CRAN maintainers,
I would be willing to merely remove the hyperlink and provide the URL only in 
plain text.  Just advise on your preference.


Carl




