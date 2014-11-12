# SIREn: Efficient semi-structured Information Retrieval for Lucene/Solr/Elasticsearch

## Introduction

Efficient, large scale handling of semi-structured data is increasingly an
important issue to many web and enterprise information reuse scenarios.

While Lucene has long offered these capabilities, its native capabilities are
not intended for collections of semi-structured documents (e.g., documents with
very different schemas, documents with arbitrary nested objects). For this
reason we developed SIREn - Semantic Information Retrieval Engine - a
Lucene/Solr/Elasticsearch plugin to overcome these shortcomings and efficiently index and
query arbitrary JSON documents, as well as any JSON document with an
arbitrary amount of metadata fields.

For its features, SIREn can somehow be seen as halfway between Solr (of which
it offers all the search features) and MongoDB (given it can index arbitrary
JSON documents).

SIREn is a Lucene/Solr/Elasticsearch extension for efficient semi-structured full-text search.
SIREn is not a complete application by itself, but rather a code library and API
that can easily be used to create a full-featured semi-structured search engine.

## Description

The SIREn project is composed of the following modules:

* **siren-parent**: This module provides the parent pom that defines
configuration shared across all the other modules.

* **siren-core**: This module provides the core functionality of SIREn such
as the low-level indexing and search APIs.

* **siren-qparser**: This module provides a number of query parsers to easily
create complex queries through rich query languages.

* **siren-solr**: This module provides a plugin for Solr that integrates the core
functionality and the query language of SIREn into the Solr API.

* **siren-solr-facet**: This module provides an implementation of SIREn nested
faceting for Solr

* **siren-elasticsearch**: This module provides a plugin for Elasticsearch that 
integrates the core functionality and the query language of SIREn into the 
Elasticsearch API.

* **siren-lucene-demo**: This module provides a demonstration of the integration of
SIREn with Lucene.

* **siren-solr-demo**: This module provides a demonstration of the integration of
SIREn with Solr.

* **siren-elasticsearch-demo**: This module provides a demonstration of the integration of
SIREn with Elasticsearch.

	

## Reference

If you are using SIREn for your scientific work, please cite the following article
as follow:

> Renaud Delbru, Stephane Campinas, Giovanni Tummarello, **Searching web data: An
> entity retrieval and high-performance indexing model**, *In Web Semantics:
> Science, Services and Agents on the World Wide Web*, ISSN 1570-8268,
> [10.1016/j.websem.2011.04.004](http://www.sciencedirect.com/science/article/pii/S1570826811000230).

## Resources

SIREn web site:
  http://siren.solutions/

You can download SIREn at:
  http://siren.solutions/

Please join the SIREn-User mailing list by subscribing at:
  https://groups.google.com/a/sindicetech.com/forum/embed/?place=forum/siren-user

- - -

Copyright (c) 2014, Sindice Limited. All Rights Reserved.
