/**
 * Set of {@link org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor}
 * used to alter the {@link org.apache.lucene.queryparser.flexible.core.nodes.QueryNode}
 * tree created by the {@link com.sindicetech.siren.qparser.keyword.KeywordSyntaxParser}.
 *
 * The processors are used in sequence in a pipeline {@link com.sindicetech.siren.qparser.keyword.processors.KeywordQueryNodeProcessorPipeline}.
 * The order of the processors are crucial for the processing, and it is the
 * following:
 * <ul>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.TopLevelQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.AllowTwigProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.WildcardNodeQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.RootLevelTwigQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.ChangeDefaultOperatorNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.WildcardQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processorsFuzzyQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.QNamesProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.PhraseQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.OpenRangeQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.DatatypeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.NodeNumericQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.NodeNumericRangeQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.TermRangeQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.LowercaseExpandedTermsQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.DatatypeAnalyzerProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.PhraseSlopQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.DefaultPhraseSlopQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.AllowFuzzyAndWildcardProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.AllowLeadingWildcardProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.GroupQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.core.processors.NoChildOptimizationQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.core.processors.RemoveDeletedQueryNodesProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.RemoveEmptyNonLeafQueryNodeProcessor} </li>
 * <li> {@link org.apache.lucene.queryparser.flexible.standard.processors.BooleanSingleChildOptimizationQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.MultiNodeTermRewriteMethodProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.NodeBooleanQueryNodeProcessor} </li>
 * <li> {@link com.sindicetech.siren.qparser.keyword.processors.NotSupportedQueryProcessor} </li>
 * </ul>
 */
package com.sindicetech.siren.qparser.keyword.processors;
