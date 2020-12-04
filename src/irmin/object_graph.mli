(*
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

(** Graphs. *)

module type S = sig
  include Graph.Sig.I
  (** Directed graph *)

  include Graph.Oper.S with type g := t
  (** Basic operations. *)

  (** Topological traversal *)
  module Topological : sig
    val fold : (vertex -> 'a -> 'a) -> t -> 'a -> 'a
  end

  val vertex : t -> vertex list
  (** Get all the vertices. *)

  val edges : t -> (vertex * vertex) list
  (** Get all the relations. *)

  val closure :
    ?depth:int ->
    pred:(vertex -> vertex list Lwt.t) ->
    min:vertex list ->
    max:vertex list ->
    unit ->
    t Lwt.t
  (** [closure ?depth ~pred ~min ~max ()] creates the transitive closure graph
      of [max] using the predecessor relation [pred]. The graph is bounded by
      the [min] nodes and by [depth].

      {b Note:} Both [min] and [max] are subsets of the output graph. *)

  val iter :
    ?cache_size:int ->
    ?depth:int ->
    pred:(vertex -> vertex list Lwt.t) ->
    min:vertex list ->
    max:vertex list ->
    node:(vertex -> unit Lwt.t) ->
    ?edge:(vertex -> vertex -> unit Lwt.t) ->
    skip:(vertex -> bool Lwt.t) ->
    rev:bool ->
    unit ->
    unit Lwt.t
  (** [iter ?depth ~pred ~min ~max ~node ?edge ~skip ~rev ()] iterates over the
      transitive closure of the directed acyclic graph rooted at the [max]
      nodes, using the predecessor relation [pred] and bounded by the [min]
      nodes, by [depth] and by calls to [skip].

      It applies two functions on each node while traversing the graph: [node n]
      on a node [n] and [edge n predecessor_of_n] on the all the directed edges
      starting from [n]. [edge n p] is always applied after [node n].

      If [n] is in [min], [edge n predecessor_of_n] is not applied , but [node n]
      and [edge successor_of_n n] are still applied.

      If a node [n] is skipped or at maximum depth then neither [node n] nor
      [edge n predecessor_of_n] are applied, but [edge successor_of_n n] still
      is.

      If [rev] is false then the graph is traversed in topological order. If
      [rev] is true (the default) then the graph is traversed in the reverse
      order: [node n] is applied only after it was applied on all its
      predecessors.

      [cache_size] is the size of the LRU cache used to store nodes already
      seen. If [None] (by default) every traversed nodes is stored (and thus no
      entries are never removed from the LRU). *)

  val output :
    Format.formatter ->
    (vertex * Graph.Graphviz.DotAttributes.vertex list) list ->
    (vertex * Graph.Graphviz.DotAttributes.edge list * vertex) list ->
    string ->
    unit
  (** [output ppf vertices edges name] creates and dumps the graph contents on
      [ppf]. The graph is defined by its [vertices] and [edges]. [name] is the
      name of the output graph. *)

  val min : t -> vertex list
  (** Compute the minimum vertices. *)

  val max : t -> vertex list
  (** Compute the maximum vertices. *)

  type dump = vertex list * (vertex * vertex) list
  (** Expose the graph internals. *)

  val export : t -> dump
  (** Expose the graph as a pair of vertices and edges. *)

  val import : dump -> t
  (** Import a graph. *)

  module Dump : Type.S with type t = dump
  (** The base functions over graph internals. *)
end

module type HASH = sig
  include Type.S

  val short_hash : t -> int
end

(** Build a graph. *)
module Make (Hash : HASH) (Branch : Type.S) :
  S
    with type V.t =
          [ `Contents of Hash.t
          | `Node of Hash.t
          | `Commit of Hash.t
          | `Branch of Branch.t ]
