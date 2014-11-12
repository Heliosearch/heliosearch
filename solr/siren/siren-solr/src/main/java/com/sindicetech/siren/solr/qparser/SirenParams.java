/**
 * Copyright (c) 2014, Sindice Limited. All Rights Reserved.
 *
 * This file is part of the SIREn project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.sindicetech.siren.solr.qparser;

/**
 * A collection of params used in SirenRequestHandler,
 * both for Plugin initialization and for Requests.
 */
public class SirenParams {

  /** Query and init param for query fields */
  public static String QF = "qf";

  /** Init param for qname mapping file */
  public static String QNAMES = "qnames";

  /** Param for allowing leading wildcard */
  public static String ALLOW_LEADING_WILDCARD = "allowLeadingWildcard";

}
