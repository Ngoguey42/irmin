(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018 Dynamic Ledger Solutions, Inc. <contact@tezos.com>     *)
(* Copyright (c) 2018-2021 Nomadic Labs <contact@nomadic-labs.com>           *)
(* Copyright (c) 2018-2021 Tarides <contact@tarides.com>                     *)
(* Copyright (c) 2020 Metastate AG <hello@metastate.dev>                     *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

module type VIEW = sig
  (** The type for context views. *)
  type t

  (** The type for context keys. *)
  type key

  (** The type for context values. *)
  type value

  (** The type for context trees. *)
  type tree

  (** {2 Getters} *)

  (** [mem t k] is an Lwt promise that resolves to [true] iff [k] is bound
      to a value in [t]. *)
  val mem : t -> key -> bool Lwt.t

  (** [mem_tree t k] is like {!mem} but for trees. *)
  val mem_tree : t -> key -> bool Lwt.t

  (** [find t k] is an Lwt promise that resolves to [Some v] if [k] is
      bound to the value [v] in [t] and [None] otherwise. *)
  val find : t -> key -> value option Lwt.t

  (** [find_tree t k] is like {!find} but for trees. *)
  val find_tree : t -> key -> tree option Lwt.t

  (** [list t key] is the list of files and sub-nodes stored under [k] in [t].
      The result order is not specified but is stable.

      [offset] and [length] are used for pagination. *)
  val list :
    t -> ?offset:int -> ?length:int -> key -> (string * tree) list Lwt.t

  (** {2 Setters} *)

  (** [add t k v] is an Lwt promise that resolves to [c] such that:

    - [k] is bound to [v] in [c];
    - and [c] is similar to [t] otherwise.

    If [k] was already bound in [t] to a value that is physically equal
    to [v], the result of the function is a promise that resolves to
    [t]. Otherwise, the previous binding of [k] in [t] disappears. *)
  val add : t -> key -> value -> t Lwt.t

  (** [add_tree] is like {!add} but for trees. *)
  val add_tree : t -> key -> tree -> t Lwt.t

  (** [remove t k v] is an Lwt promise that resolves to [c] such that:

    - [k] is unbound in [c];
    - and [c] is similar to [t] otherwise. *)
  val remove : t -> key -> t Lwt.t

  (** {2 Folding} *)

  (** [fold ?depth t root ~init ~f] recursively folds over the trees
      and values of [t]. The [f] callbacks are called with a key relative
      to [root]. [f] is never called with an empty key for values; i.e.,
      folding over a value is a no-op.

      Elements are traversed in lexical order of keys.

      The depth is 0-indexed. If [depth] is set (by default it is not), then [f]
      is only called when the conditions described by the parameter is true:

      - [Eq d] folds over nodes and contents of depth exactly [d].
      - [Lt d] folds over nodes and contents of depth strictly less than [d].
      - [Le d] folds over nodes and contents of depth less than or equal to [d].
      - [Gt d] folds over nodes and contents of depth strictly more than [d].
      - [Ge d] folds over nodes and contents of depth more than or equal to [d]. *)
  val fold :
    ?depth:[`Eq of int | `Le of int | `Lt of int | `Ge of int | `Gt of int] ->
    t ->
    key ->
    init:'a ->
    f:(key -> tree -> 'a -> 'a Lwt.t) ->
    'a Lwt.t
end

module type TREE = sig
  (** [Tree] provides immutable, in-memory partial mirror of the
      context, with lazy reads and delayed writes. The trees are Merkle
      trees that carry the same hash as the part of the context they
      mirror.

      Trees are immutable and non-persistent (they disappear if the
      host crash), held in memory for efficiency, where reads are done
      lazily and writes are done only when needed, e.g. on
      [Context.commit]. If a key is modified twice, only the last
      value will be written to disk on commit. *)

  (** The type for context views. *)
  type t

  (** The type for context trees. *)
  type tree

  include VIEW with type t := tree and type tree := tree

  (** [empty _] is the empty tree. *)
  val empty : 'a -> tree

  (** [is_empty t] is true iff [t] is [empty _]. *)
  val is_empty : tree -> bool

  (** [kind t] is [t]'s kind. It's either a tree node or a leaf
      value. *)
  val kind : tree -> [`Value | `Tree]

  (** [to_value t] is an Lwt promise that resolves to [Some v] if [t]
      is a leaf tree and [None] otherwise. It is equivalent to [find t
      []]. *)
  val to_value : tree -> value option Lwt.t

  (** [of_value _ v] is an Lwt promise that resolves to the leaf tree
      [v]. Is is equivalent to [add (empty _) [] v]. *)
  val of_value : 'a -> value -> tree Lwt.t

  (** [hash t] is [t]'s Merkle hash. *)
  val hash : tree -> Context_hash.t

  (** [equal x y] is true iff [x] and [y] have the same Merkle hash. *)
  val equal : tree -> tree -> bool

  (** {2 Caches} *)

  (** [clear ?depth t] clears all caches in the tree [t] for subtrees with a
      depth higher than [depth]. If [depth] is not set, all of the subtrees are
      cleared. *)
  val clear : ?depth:int -> tree -> unit
end

module type MEM = sig
  include VIEW with type key = string list and type value = bytes

  module Tree : sig
    include
      TREE
        with type t := t
         and type key := key
         and type value := value
         and type tree := tree

    (** [pp] is the pretty-printer for trees. *)
    val pp : Format.formatter -> tree -> unit

    (** {2 Data Encoding} *)

    (** The type for in-memory, raw contexts. *)
    type raw = [`Value of bytes | `Tree of raw TzString.Map.t]

    (** [raw_encoding] is the data encoding for raw trees. *)
    val raw_encoding : raw Data_encoding.t

    (** [to_raw t] is an Lwt promise that resolves to a raw tree
        equivalent to [t]. *)
    val to_raw : tree -> raw Lwt.t

    (** [of_raw t] is the tree equivalent to the raw tree [t]. *)
    val of_raw : raw -> tree

    (** The type of tree for which to build a shallow tree with [shallow] *)
    type kinded_hash := [`Contents of Context_hash.t | `Node of Context_hash.t]

    type repo

    val make_repo : repo Lwt.t

    val shallow : repo -> kinded_hash -> tree
  end
end

module type S = sig
  (** Tezos - Versioned, block indexed (key x value) store *)

  (* type error +=
   *   | Cannot_create_file of string
   *   | Cannot_open_file of string
   *   | Cannot_find_protocol
   *   | Suspicious_file of int *)

  (** {2 Generic interface} *)

  module type S = sig
    (** @inline *)
    include MEM
  end

  include S

  type context = t

  (** A block-indexed (key x value) store directory.  *)
  type index

  val init :
    ?patch_context:(context -> context tzresult Lwt.t) ->
    ?readonly:bool ->
    string ->
    index Lwt.t

  (** Close the index. Does not fail when the context is already closed. *)
  val close : index -> unit Lwt.t

  (** Sync the context with disk. Only useful for read-only instances.
    Does not fail when the context is not in read-only mode. *)
  val sync : index -> unit Lwt.t

  val compute_testchain_chain_id : Block_hash.t -> Chain_id.t

  val compute_testchain_genesis : Block_hash.t -> Block_hash.t

  val commit_genesis :
    index ->
    chain_id:Chain_id.t ->
    time:Time.Protocol.t ->
    protocol:Protocol_hash.t ->
    Context_hash.t tzresult Lwt.t

  (* Signature modified *)
  val commit_test_chain_genesis :
    context -> (Time.Protocol.t * int32 * Block_hash.t) -> Context_hash.t Lwt.t

  (* (\** [merkle_tree t leaf_kind key] returns a Merkle proof for [key] (i.e.
   *   whose hashes reach [key]). If [leaf_kind] is [Block_services.Hole], the value
   *   at [key] is a hash. If [leaf_kind] is [Block_services.Raw_context],
   *   the value at [key] is a [Block_services.raw_context]. Values higher
   *   in the returned tree are hashes of the siblings on the path to
   *   reach [key]. *\)
   * val merkle_tree :
   *   t ->
   *   Block_services.merkle_leaf_kind ->
   *   key ->
   *   Block_services.merkle_tree Lwt.t *)

  (** {2 Accessing and Updating Versions} *)

  (* (\** [restore_integrity ppf index] attempts to restore the context
   *   integrity of [index]. Returns [None] when nothing needs to be fixed and
   *   [Some n] with [n] the number of entries fixed. If needs be, the
   *   progress might be printed via [ppf]
   *   If the context integrity cannot be restored, [Failure msg] is thrown. *\)
   * val restore_integrity : ?ppf:Format.formatter -> index -> int option tzresult *)

  val exists : index -> Context_hash.t -> bool Lwt.t

  val checkout : index -> Context_hash.t -> context option Lwt.t

  val checkout_exn : index -> Context_hash.t -> context Lwt.t

  val hash : time:Time.Protocol.t -> ?message:string -> t -> Context_hash.t

  val commit :
    time:Time.Protocol.t -> ?message:string -> context -> Context_hash.t Lwt.t

  val set_head : index -> Chain_id.t -> Context_hash.t -> unit Lwt.t

  val set_master : index -> Context_hash.t -> unit Lwt.t

  (** {2 Predefined Fields} *)

  val get_protocol : context -> Protocol_hash.t Lwt.t

  val add_protocol : context -> Protocol_hash.t -> context Lwt.t

  val get_test_chain : context -> Test_chain_status.t Lwt.t

  val add_test_chain : context -> Test_chain_status.t -> context Lwt.t

  val remove_test_chain : context -> context Lwt.t

  val fork_test_chain :
    context ->
    protocol:Protocol_hash.t ->
    expiration:Time.Protocol.t ->
    context Lwt.t

  val clear_test_chain : index -> Chain_id.t -> unit Lwt.t

  val find_predecessor_block_metadata_hash :
    context -> Block_metadata_hash.t option Lwt.t

  val add_predecessor_block_metadata_hash :
    context -> Block_metadata_hash.t -> context Lwt.t

  val find_predecessor_ops_metadata_hash :
    context -> Operation_metadata_list_list_hash.t option Lwt.t

  val add_predecessor_ops_metadata_hash :
    context -> Operation_metadata_list_list_hash.t -> context Lwt.t

  (** {2 Context dumping} *)

  (* module Protocol_data_legacy : sig
   *   type t = Int32.t * data
   *
   *   and info = {author : string; message : string; timestamp : Time.Protocol.t}
   *
   *   and data = {
   *     info : info;
   *     protocol_hash : Protocol_hash.t;
   *     test_chain_status : Test_chain_status.t;
   *     data_key : Context_hash.t;
   *     predecessor_block_metadata_hash : Block_metadata_hash.t option;
   *     predecessor_ops_metadata_hash :
   *       Operation_metadata_list_list_hash.t option;
   *     parents : Context_hash.t list;
   *   }
   *
   *   val to_bytes : t -> Bytes.t
   *
   *   val of_bytes : Bytes.t -> t option
   *
   *   val encoding : t Data_encoding.t
   * end
   *
   * module Block_data_legacy : sig
   *   type t = {block_header : Block_header.t; operations : Operation.t list list}
   *
   *   val to_bytes : t -> Bytes.t
   *
   *   val of_bytes : Bytes.t -> t option
   *
   *   val encoding : t Data_encoding.t
   * end
   *
   * module Pruned_block_legacy : sig
   *   type t = {
   *     block_header : Block_header.t;
   *     operations : (int * Operation.t list) list;
   *     operation_hashes : (int * Operation_hash.t list) list;
   *   }
   *
   *   val encoding : t Data_encoding.t
   *
   *   val to_bytes : t -> Bytes.t
   *
   *   val of_bytes : Bytes.t -> t option
   * end *)

  (* val dump_context :
   *   index -> Context_hash.t -> fd:Lwt_unix.file_descr -> int tzresult Lwt.t
   *
   * val restore_context :
   *   index ->
   *   expected_context_hash:Context_hash.t ->
   *   nb_context_elements:int ->
   *   fd:Lwt_unix.file_descr ->
   *   unit tzresult Lwt.t *)

  (* val legacy_restore_context :
   *   ?expected_block:string ->
   *   index ->
   *   snapshot_file:string ->
   *   handle_block:
   *     (History_mode.Legacy.t ->
   *     Block_hash.t * Pruned_block_legacy.t ->
   *     unit tzresult Lwt.t) ->
   *   handle_protocol_data:(Protocol_data_legacy.t -> unit tzresult Lwt.t) ->
   *   block_validation:
   *     (Block_header.t option ->
   *     Block_hash.t ->
   *     Pruned_block_legacy.t ->
   *     unit tzresult Lwt.t) ->
   *   (Block_header.t
   *   * Block_data_legacy.t
   *   * Block_metadata_hash.t option
   *   * Tezos_crypto.Operation_metadata_hash.t list list option
   *   * Block_header.t option
   *   * History_mode.Legacy.t)
   *   tzresult
   *   Lwt.t
   *
   * val legacy_read_metadata :
   *   snapshot_file:string -> (string * History_mode.Legacy.t) tzresult Lwt.t
   *
   * (\* Interface exposed for the lib_store/legacy_store *\)
   * val legacy_restore_contexts :
   *   index ->
   *   filename:string ->
   *   ((Block_hash.t * Pruned_block_legacy.t) list -> unit tzresult Lwt.t) ->
   *   (Block_header.t option ->
   *   Block_hash.t ->
   *   Pruned_block_legacy.t ->
   *   unit tzresult Lwt.t) ->
   *   (Block_header.t
   *   * Block_data_legacy.t
   *   * Block_metadata_hash.t option
   *   * Operation_metadata_hash.t list list option
   *   * History_mode.Legacy.t
   *   * Block_header.t option
   *   * Block_hash.t list
   *   * Protocol_data_legacy.t list)
   *   tzresult
   *   Lwt.t *)

  (* Signature modified *)
  val retrieve_commit_info :
    index ->
    Context_hash.t ->
    (Protocol_hash.t
    * string
    * string
    * Time.Protocol.t
    * Test_chain_status.t
    * Context_hash.t
    * Block_metadata_hash.t option
    * Operation_metadata_list_list_hash.t option
    * Context_hash.t list)
    tzresult
    Lwt.t

  (* val check_protocol_commit_consistency :
   *   index ->
   *   expected_context_hash:Context_hash.t ->
   *   given_protocol_hash:Protocol_hash.t ->
   *   author:string ->
   *   message:string ->
   *   timestamp:Time.Protocol.t ->
   *   test_chain_status:Test_chain_status.t ->
   *   predecessor_block_metadata_hash:Block_metadata_hash.t option ->
   *   predecessor_ops_metadata_hash:Operation_metadata_list_list_hash.t option ->
   *   data_merkle_root:Context_hash.t ->
   *   parents_contexts:Context_hash.t list ->
   *   bool Lwt.t *)

  (**/**)

  (** {b Warning} For testing purposes only *)

  (* val legacy_get_protocol_data_from_header :
   *   index -> Block_header.t -> Protocol_data_legacy.t Lwt.t
   *
   * val legacy_dump_snapshot :
   *   index ->
   *   Block_header.t
   *   * Block_data_legacy.t
   *   * Block_metadata_hash.t option
   *   * Operation_metadata_hash.t list list option
   *   * History_mode.Legacy.t
   *   * (Block_header.t ->
   *     (Pruned_block_legacy.t option * Protocol_data_legacy.t option) tzresult
   *     Lwt.t) ->
   *   filename:string ->
   *   unit tzresult Lwt.t *)

  (* val validate_context_hash_consistency_and_commit :
   *   data_hash:Context_hash.t ->
   *   expected_context_hash:Context_hash.t ->
   *   timestamp:Time.Protocol.t ->
   *   test_chain:Test_chain_status.t ->
   *   protocol_hash:Protocol_hash.t ->
   *   message:string ->
   *   author:string ->
   *   parents:Context_hash.t list ->
   *   predecessor_block_metadata_hash:Block_metadata_hash.t option ->
   *   predecessor_ops_metadata_hash:Operation_metadata_list_list_hash.t option ->
   *   index:index ->
   *   bool Lwt.t
   *
   * (\** Offline integrity checking and statistics for contexts. *\)
   * module Checks : sig
   *   module Pack : Irmin_pack.Checks.S
   *
   *   module Index : Index.Checks.S
   * end *)
end
