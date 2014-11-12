# Example Solr/SIREn Home Directory

This directory is provided as an example of what a "Solr/SIREn Home" directory
should look like.

It's not strictly necessary that you copy all of the files in this
directory when setting up a new instance of Solr/SIREn, but it is recommended.

## Basic Directory Structure

The Solr/SIREn Home directory typically contains the following...

	solr.xml

This is the primary configuration file Solr looks for when starting.
This file specifies high level configuration options that should be used for
all SolrCores.

## Individual SolrCore Instance Directories *

solr.xml is configured to look for SolrCore Instance Directories
in any path, simple sub-directories of the Solr Home Dir using relative paths
are common for many installations.  In this directory you can see the
"./collection1" SIREn Instance Directory.

## A Shared 'lib' Directory *

Although solr.xml can be configured with an optional "sharedLib" attribute 
that can point to any path, it is common to use a "./lib" sub-directory of the 
Solr Home Directory.

## ZooKeeper Files

When using SolrCloud using the embedded ZooKeeper option for Solr, it is
common to have a "zoo.cfg" file and "zoo_data" directories in the Solr Home
Directory.  Please see the SolrCloud wiki page for more details...

	https://wiki.apache.org/solr/SolrCloud

- - -

Copyright (c) 2014, Sindice Limited. All Rights Reserved.
