/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * The subsystem of the auth package concerned with controlling user operations at a fine grained (sub-authz) level.
 *
 * Capabilities are a lower level concept than authz permissions and operations may require zero or more capabilities
 * in order to be performed. For instance, for a non-superuser to be able to query from a given table, she must be
 * granted SELECT permission on either the table, its enclosing keyspace or all keyspaces. This grants read access to
 * the table to that user, regardless of the specific query. Capabilities can be used to further categorize the kinds
 * of operations (in this example, queries) can be performed using the resource (the table). Examples of the built-in
 * capabilities include performing multi partition and range queries, queries which use native or custom secondary
 * indexes, capabilities representing reads and writes at the various consistency levels and so on. Ultimately, these
 * capabilities may be used in restrictions whereby admins can restrict particular roles from using specific
 * capabilities with named resources. I.e. to limit the kinds of operations that certain users are allowed to perform,
 * at a much finer granularity than is possible using permissions. Capabilities are namedspaced, so that the default
 * capabilities can be extended by providing additional implementations. This may be useful to implementors of indexes,
 * triggers and the like.
 *
 * {@link org.apache.cassandra.auth.capability.Capability} abstract base for representing capabilities, identified by
 * a domain and name.
 *
 * {@link org.apache.cassandra.auth.capability.Capabilities} provides utility methods for working with Capabilities.
 * Also defines the system default capabilities.
 *
 * {@link org.apache.cassandra.auth.capability.CapabilitySet} represents a collection of capabilities, grouped by
 * domain. Used to compare sets of required and restricted capabilities at execution time, to check whether any of the
 * capabilities required by the operation have been restricted for the active client.
 *
 * {@link org.apache.cassandra.auth.capability.RestrictionsCache} is a standard
 * {@link org.apache.cassandra.auth.AuthCache} which caches the restricted capabilities (as a
 * {@link org.apache.cassandra.auth.capability.CapabilitySet} for a key of
 * {@link org.apache.cassandra.auth.RoleResource} and {@link org.apache.cassandra.auth.IResource}
 *
 * {@link org.apache.cassandra.auth.capability.ICapabilityManager} is the core interface defining the methods for
 * managing and looking up capability restrictions.
 *
 * {@link org.apache.cassandra.auth.capability.CassandraCapabilityManager} is the default implementation of
 * {@link org.apache.cassandra.auth.capability.ICapabilityManager}, which uses tables in the system_auth keyspace to
 * store metadata about capability restrictions.
 *
 * {@link org.apache.cassandra.auth.capability.TableBasedRestrictionHandler} is used under the hood by
 * {@link org.apache.cassandra.auth.capability.CassandraCapabilityManager} to access the underlying data tables.
 * This abstraction exists to make unit testing easier by enabling its queries to be executed in non-distributed mode
 * during tests, without going via {@link org.apache.cassandra.service.StorageProxy}.
 **/
package org.apache.cassandra.auth.capability;
