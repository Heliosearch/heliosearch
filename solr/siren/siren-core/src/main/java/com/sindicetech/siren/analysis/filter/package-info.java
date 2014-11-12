/**
 * Token's filters used for term processing, URI processing and JSON indexing.
 *
 * <h2>Introduction</h2>
 *
 * This package contains a number of token's filters that are used to
 * pre-process URI tokens or to index JSON data.
 *
 * <h3>JSON Indexing</h3>
 *
 * <ul>
 * <li> {@link com.sindicetech.siren.analysis.filter.DatatypeAnalyzerFilter}
 * analyzes the token given its datatype.
 * <li> {@link com.sindicetech.siren.analysis.filter.PositionAttributeFilter}
 * computes the relative position of the token within a node.
 * <li> {@link com.sindicetech.siren.analysis.filter.SirenPayloadFilter} encodes
 * the node information into the token payload.
 * </ul>
 *
 * <h3>URI Processing</h3>
 *
 * <ul>
 * <li> {@link com.sindicetech.siren.analysis.filter.URIDecodingFilter} decodes
 * special URI characters.
 * <li> {@link com.sindicetech.siren.analysis.filter.URILocalnameFilter} extracts
 * the local name of a URI.
 * <li> {@link com.sindicetech.siren.analysis.filter.URINormalisationFilter} breaks
 * an URI into smaller components.
 * <li> {@link com.sindicetech.siren.analysis.filter.URITrailingSlashFilter} removes
 * the trailing slash of a URI.
 * <li> {@link com.sindicetech.siren.analysis.filter.MailtoFilter} extracts the mail
 * address from a mailto URI scheme.
 * </ul>
 *
 * <h3>Term Processing</h3>
 *
 * <ul>
 * <li> {@link com.sindicetech.siren.analysis.filter.ASCIIFoldingExpansionFilter}
 * expands tokens containing diacritical mark and other special characters.
 * </ul>
 *
 */
package com.sindicetech.siren.analysis.filter;


