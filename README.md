# zap2xml
Zap2XML - Python port - Automate zap2it TV guide to XMLTV

Just a messy port from the zap2xml.pl file that I found [here](https://github.com/jef/zap2xml).
I used lots of AI assist to port it, as I'm neither a Perl nor Python expert.  I ported it because I just couldn't get the Perl version to work.  Kept getting Time::HiRes error and AI told me it's not supported in Windows and it must be caused by a dependency.  No amount of Google or AI was solving it, so I gave up and ported it instead.

It could use some optimizations, but so far, seems to be working.

It's currently working with tvlistings.gracenote.com.

The original project and command-line switches are [here](https://web.archive.org/web/20200426004001/zap2xml.awardspace.info/).