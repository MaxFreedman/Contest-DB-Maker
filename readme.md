# Contest DB Maker

Quick and dirty (and bad) toolset to make cq contest sql databases. Built in 15 mins with python and old code I had. Made to commit to a postgresql db.

## Description

makelistcq.py will actually scrape URLs to make a list of unique calls over a year range for given links. I would only making the list for the contests you actually wanna import, otherwise it will be big. Note that since I just grab \<td> data, you will get non callsign lines, though for data this big it doesn't reallly matter (I don't wanna regex remove them).

wpxscrape.py is an example python scraper that scrapes the cabrillo, makes it into a usable format, and then commits that to a db. I use postgres so its built for that, and the columns are built for CQWPX. It works well enough, though even with async http you have to run it quite slow (at least I have).

## Usage

I haven't found anything definitive as to whether or not this is allowed by the WWROF, so use at own risk. The website seems to handle the traffic fine. 

If you want to make improvements, just fork/steal my code and credit me. With the nature of contest log releases, you should probably make a better script for actual db maintenance.

If you are affiliated with the WWROF/CQWW/CQWPX/etc and don't like this, email me and I will take it down.


73 de Max N4ML

