# Example Solr/SIREn Instance Directory

This directory is provided as an example of what a Solr/SIREn instance
directory should look like.

It's not strictly necessary that you copy all of the files in this
directory when setting up a new instance of Solr/SIREn, but it is recommended.

# Basic Directory Structure


The Solr/SIREn Home directory typically contains the following files or
sub-directories:

## core.properties

Exploration of the core tree terminates when a file named core.properties is
encountered. Discovery of a file of that name is assumed to define the root of
a core. The present core.properties file contains one single required entry:

- name: the name of the core.

## conf/

This directory is mandatory and must contain your solrconfig.xml, schema.xml
and dataypes.xml. Any other optional configuration files such as qnames.txt
would also be kept here.

## data/

This directory is the default location where Solr/SIREn will keep your
index, and is used by the replication scripts for dealing with
snapshots. You can override this location in the
conf/solrconfig.xml. Solr will create this directory if it does not
already exist.

## lib/

This directory is optional. If it exists, Solr will load any Jars
found in this directory and use them to resolve any "plugins"
specified in your solrconfig.xml or schema.xml (ie: Analyzers,
Request Handlers, etc...). Alternatively you can use the <lib>
syntax in conf/solrconfig.xml to direct Solr to your plugins.  See
the example conf/solrconfig.xml file for details.

- - -

Copyright (c) 2014, Sindice Limited. All Rights Reserved.