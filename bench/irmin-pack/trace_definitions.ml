(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
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

(** Traces file format definitions.

    This file is meant to be used from Tezos. OCaml version 4.09 and the 32bit
    architecture should be supported.

    {3 Traces Workflow}

    {v
            (tezos-node) ------> {not yet implemented} ---------\
                  |                                             |
                  v                                             |
          [raw actions trace] -> (manage_actions.exe summarise) |
                  |                            |                |
                  |                            v                |
                  |                 [raw_actions_summary.json]  |
                  |                            |                |
                  |                            v                |
                  |                   (pandas / matplotlib)     |
                  v                                             |
  (manage_actions.exe to-replayable)                            |
                  |                                             |
                  v                                             |
       [replayable actions trace]                               |
                  |                                             |
                  v                                             v
             (tree.exe) -----------------------------> [stat trace]
                                                            |
                                                            v
                                               (manage_stats.exe summarise)
                                                            |
                                                            v
                                                  [stat_summary.json]
                                                            |
                                                            v
                                     (pandas / matplotlib / manage_stats.exe pp)
    v} *)

let seq_take count_expected seq =
  let rec aux seq took_rev count_sofar =
    if count_sofar = count_expected then List.rev took_rev
    else
      match seq () with
      | Seq.Nil -> List.rev took_rev
      | Cons (v, seq) -> aux seq (v :: took_rev) (count_sofar + 1)
  in
  aux seq [] 0

module Raw_actions_trace = struct
  module V0 = struct
    let version = 0

    type header = unit [@@deriving repr]
    type key = string list [@@deriving repr]
    type hash = string [@@deriving repr]
    type commit_hash = hash [@@deriving repr]
    type message = string [@@deriving repr]
    type tracker = int64 [@@deriving repr]

    type tracker_range = { first_tracker : tracker; count : int }
    [@@deriving repr]

    type tree = tracker [@@deriving repr]
    type trees_with_contiguous_trackers = tracker_range [@@deriving repr]
    type context = tracker [@@deriving repr]
    type value = bytes [@@deriving repr]
    type md5 = Digestif.MD5.t

    let md5_t : md5 Repr.t =
      Repr.map Repr.string Digestif.MD5.of_raw_string Digestif.MD5.to_raw_string

    type step = string [@@deriving repr]

    type depth =
      [ `Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int ]
    [@@deriving repr]

    type block_level = int32 [@@deriving repr]
    type time_protocol = int64 [@@deriving repr]
    type merkle_leaf_kind = [ `Hole | `Raw_context ] [@@deriving repr]
    type chain_id = string [@@deriving repr]

    type test_chain_status_forking = {
      protocol : hash;
      expiration : time_protocol;
    }
    [@@deriving repr]

    type test_chain_status_running = {
      chain_id : chain_id;
      genesis : hash;
      protocol : hash;
      expiration : time_protocol;
    }
    [@@deriving repr]

    type test_chain_status =
      | Not_running
      | Forking of test_chain_status_forking
      | Running of test_chain_status_running
    [@@deriving repr]

    type system_wide_timestamp = float [@@deriving repr]

    type timestamp_bounds = {
      before : system_wide_timestamp;
      after : system_wide_timestamp;
    }
    [@@deriving repr]

    type ('input, 'output) fn = 'input * 'output [@@deriving repr]

    module Tree = struct
      type raw = [ `Value of value | `Tree of (step * raw) list ]
      [@@deriving repr]

      type t =
        (* [_o __ ___] *)
        | Empty of (unit, tree) fn
        | Of_raw of (raw, tree) fn
        | Of_value of (value, tree) fn
        (* [i_ __ ___] *)
        | Mem of (tree * key, bool) fn
        | Mem_tree of (tree * key, bool) fn
        | Find of (tree * key, md5 option (* recording hash of bytes *)) fn
        | Is_empty of (tree, bool) fn
        | Kind of (tree, [ `Tree | `Value ]) fn
        | Hash of (tree, hash) fn
        | Equal of (tree * tree, bool) fn
        | To_value of (tree, md5 option (* recording hash of bytes *)) fn
        | Clear of (int option * tree, unit) fn
        (* [io __ ___] *)
        | Find_tree of (tree * key, tree option) fn
        | List of
            ( tree * int option * int option,
              trees_with_contiguous_trackers (* not recording the steps *) )
            fn
        | Add of (tree * key * value, tree) fn
        | Add_tree of (tree * key * tree, tree) fn
        | Remove of (tree * key, tree) fn
        | Fold_start of depth option * tree * key
        | Fold_step_enter of tree
        | Fold_step_exit of tree
        | Fold_end of int
        (* loosely tracked *)
        | Shallow
        | To_raw
        | Pp
      [@@deriving repr]
    end

    type row =
      (* [** __ ___] *)
      | Tree of Tree.t
      (* [_o i_ ___] *)
      | Find_tree of (context * key, tree option) fn
      | List of
          ( context * int option * int option,
            trees_with_contiguous_trackers (* not recording the steps *) )
          fn
      | Fold_start of depth option * context * key
      | Fold_step_enter of tree
      | Fold_step_exit of tree
      | Fold_end of int
      (* [i_ io ___]*)
      | Add_tree of (context * key * tree, tree) fn
      (* [__ i_ ___] *)
      | Mem of (context * key, bool) fn
      | Mem_tree of (context * key, bool) fn
      | Find of (context * key, md5 option (* recording hash of bytes *)) fn
      | Get_protocol of (context, hash) fn
      | Hash of (time_protocol * message option * context, commit_hash) fn
      | Merkle_tree of
          ( context * merkle_leaf_kind * step list,
            unit (* not recording the block_servied.merkle_tree *) )
          fn
      | Find_predecessor_block_metadata_hash of (context, hash option) fn
      | Find_predecessor_ops_metadata_hash of (context, hash option) fn
      | Get_test_chain of (context, test_chain_status) fn
      (* [__ __ i__] *)
      | Exists of (commit_hash, bool) fn * timestamp_bounds
      | Retrieve_commit_info of
          ( commit_hash,
            bool (* only recording is_ok of that massive tuple *) )
          fn
          * timestamp_bounds
      (* [__ io ___] *)
      | Add of (context * key * value, context) fn
      | Remove of (context * key, context) fn
      | Add_protocol of (context * hash, context) fn
      | Add_predecessor_block_metadata_hash of (context * hash, context) fn
      | Add_predecessor_ops_metadata_hash of (context * hash, context) fn
      | Add_test_chain of (context * test_chain_status, context) fn
      | Remove_test_chain of (context, context) fn
      | Fork_test_chain of (context * hash * time_protocol, context) fn
      (* [__ _o i__] *)
      | Checkout of (commit_hash, context option) fn * timestamp_bounds
      | Checkout_exn of
          (commit_hash, (context, unit) result) fn * timestamp_bounds
      (* [__ __ i_m] *)
      | Close
      | Sync of timestamp_bounds
      | Set_master of (commit_hash, unit) fn
      | Set_head of (chain_id * commit_hash, unit) fn
      | Commit_genesis of
          (chain_id * time_protocol * hash, (commit_hash, unit) result) fn
          * timestamp_bounds
      | Clear_test_chain of (chain_id, unit) fn
      (* [__ i_ __m] *)
      | Commit of
          (time_protocol * message option * context, commit_hash) fn
          * timestamp_bounds
      | Commit_test_chain_genesis of
          ( context * (time_protocol * block_level * hash),
            unit (* block header not recorded *) )
          fn
          * timestamp_bounds
      (* [__ ~~ _o_] *)
      | Init of (bool option, unit) fn
      | Patch_context_enter of context
      | Patch_context_exit of context * (context, unit) result
      (* loosely tracked *)
      | Restore_context
      | Restore_integrity
      | Dump_context
      | Check_protocol_commit_consistency
      | Validate_context_hash_consistency_and_commit
    [@@deriving repr]
  end

  module Latest = V0
  include Latest

  include Trace_common.Io (struct
    module Latest = Latest

    (** Irmin's Replayable Bootstrap Trace *)
    let magic = Trace_common.Magic.of_string "IrmRawBT"

    let get_version_converter = function
      | 0 ->
          Trace_common.Version_converter
            {
              header_t = V0.header_t;
              row_t = V0.row_t;
              upgrade_header = Fun.id;
              upgrade_row = Fun.id;
            }
      | i -> Fmt.invalid_arg "Unknown Raw_actions_trace version %d" i
  end)

  type file_type = [ `Ro | `Rw | `Misc ]

  let type_of_file p =
    let reader = open_reader p |> snd in
    let l = seq_take 10 reader in
    let ros =
      List.filter (function Init (Some true, _) -> true | _ -> false) l
    in
    let rws =
      List.filter
        (function Init (Some false, _) | Init (None, _) -> true | _ -> false)
        l
    in
    match (List.length ros, List.length rws) with
    | 1, 0 -> `Ro
    | 0, 1 -> `Rw
    | _ -> `Misc

  let trace_files_of_trace_directory ?filter prefix : (string * int) list =
    let filter =
      match filter with
      | None -> [ `Ro; `Rw; `Misc ]
      | Some v -> (v :> file_type list)
    in
    let parse_filename p =
      match String.split_on_char '.' p with
      | [ "raw_actions_trace"; pid; "trace" ] -> int_of_string_opt pid
      | _ -> None
    in
    Sys.readdir prefix
    |> Array.to_list
    |> List.filter_map (fun p ->
           match parse_filename p with
           | Some pid -> Some (Filename.concat prefix p, pid)
           | None -> None)
    |> List.filter (fun (p, _) -> List.mem (type_of_file p) filter)
end

(** [Replayable_actions_trace], a trace of Tezos's interactions with Irmin.

    {3 Interleaved Contexts and Commits}

    All the recorded operations in Tezos operate on (and create new) immutable
    records of type [context]. Most of the time, everything is linear (i.e. the
    input context to an operation is the latest output context), but there
    sometimes are several parallel chains of contexts, where all but one will
    end up being discarded.

    Similarly to contexts, commits are not always linear, i.e. a checkout may
    choose a parent that is not the latest commit.

    To solve this conundrum when replaying the trace, we need to remember all
    the [context_id -> tree] and [trace commit hash -> real commit hash] pairs
    to make sure an operation is operating on the right parent.

    In the trace, the context indices and the commit hashes are 'scoped',
    meaning that they are tagged with a boolean information indicating if this
    is the very last occurence of that value in the trace. This way we can
    discard a recorded pair as soon as possible.

    In practice, there is only 1 context and 1 commit in history, and sometimes
    0 or 2, but the code is ready for more.

    The same concept goes for trees. *)
module Replayable_actions_trace = struct
  module V1 = struct
    let version = 1

    type key = string list [@@deriving repr]
    type hash = string [@@deriving repr]
    type message = string [@@deriving repr]
    type tracker = int64 [@@deriving repr]
    type step = string [@@deriving repr]
    type value = bytes [@@deriving repr]
    type md5 = Digestif.MD5.t

    let md5_t : md5 Repr.t =
      Repr.map Repr.string Digestif.MD5.of_raw_string Digestif.MD5.to_raw_string

    type depth =
      [ `Eq of int | `Ge of int | `Gt of int | `Le of int | `Lt of int ]
    [@@deriving repr]

    type block_level = int [@@deriving repr]
    type time_protocol = int64 [@@deriving repr]
    type merkle_leaf_kind = [ `Hole | `Raw_context ] [@@deriving repr]
    type chain_id = string [@@deriving repr]

    type test_chain_status_forking = {
      protocol : hash;
      expiration : time_protocol;
    }
    [@@deriving repr]

    type test_chain_status_running = {
      chain_id : chain_id;
      genesis : hash;
      protocol : hash;
      expiration : time_protocol;
    }
    [@@deriving repr]

    type test_chain_status =
      | Not_running
      | Forking of test_chain_status_forking
      | Running of test_chain_status_running
    [@@deriving repr]

    (** [scope_instanciation] tags are used in replay to identify the situations
        where a hash is created but has already been created, i.e. when two
        commits have the same hash. *)
    type scope_start_rhs = First_instanciation | Reinstanciation
    [@@deriving repr]

    (** [scope_start] tags are used in replay to identify the situations where a
        hash is required but was never seen, e.g. the first checkout of a replay
        that starts on a snapshot. *)
    type scope_start_lhs = Instanciated | Not_instanciated [@@deriving repr]

    (** [scope_end] tags are used in replay to garbage collect the values in
        cache. *)
    type scope_end = Last_occurence | Will_reoccur [@@deriving repr]

    type tree = scope_end * tracker [@@deriving repr]
    type context = scope_end * tracker [@@deriving repr]
    type commit_hash_rhs = scope_start_rhs * scope_end * hash [@@deriving repr]
    type commit_hash_lhs = scope_start_lhs * scope_end * hash [@@deriving repr]
    type ('input, 'output) fn = 'input * 'output [@@deriving repr]

    module Tree = struct
      type raw = [ `Value of value | `Tree of (step * raw) list ]
      [@@deriving repr]

      type t =
        (* [_o __ ___] *)
        | Empty of (unit, tree) fn
        | Of_raw of (raw, tree) fn
        | Of_value of (value, tree) fn
        (* [i_ __ ___] *)
        | Mem of (tree * key, bool) fn
        | Mem_tree of (tree * key, bool) fn
        | Find of (tree * key, bool (* recording is_some *)) fn
        | Is_empty of (tree, bool) fn
        | Kind of (tree, [ `Tree | `Value ]) fn
        | Hash of (tree, unit (* not recorded *)) fn
        | Equal of (tree * tree, bool) fn
        | To_value of (tree, bool (* recording is_some *)) fn
        | Clear of (int option * tree, unit) fn
        (* [io __ ___] *)
        | Find_tree of (tree * key, tree option) fn
        | Add of (tree * key * value, tree) fn
        | Add_tree of (tree * key * tree, tree) fn
        | Remove of (tree * key, tree) fn
      [@@deriving repr]
    end

    type event =
      (* [** __ ___] *)
      | Tree of Tree.t
      (* [_o i_ ___] *)
      | Find_tree of (context * key, tree option) fn
      | Fold_start of depth option * context * key
      | Fold_step_enter of tree
      | Fold_step_exit of tree
      | Fold_end
      (* [i_ io ___]*)
      | Add_tree of (context * key * tree, tree) fn
      (* [__ i_ ___] *)
      | Mem of (context * key, bool) fn
      | Mem_tree of (context * key, bool) fn
      | Find of (context * key, bool (* recording is_some *)) fn
      | Get_protocol of (context, unit (* not recorded *)) fn
      | Hash of
          (time_protocol * message option * context, unit (* not recorded *)) fn
      | Find_predecessor_block_metadata_hash of
          (context, unit (* not recorded *)) fn
      | Find_predecessor_ops_metadata_hash of
          (context, unit (* not recorded *)) fn
      | Get_test_chain of (context, unit (* not recorded *)) fn
      (* [__ __ i__] *)
      | Exists of (commit_hash_lhs, bool) fn
      | Retrieve_commit_info of
          ( commit_hash_lhs,
            bool (* only recording is_ok of that massive tuple *) )
          fn
      (* [__ io ___] *)
      | Add of (context * key * value, context) fn
      | Remove of (context * key, context) fn
      | Add_protocol of (context * hash, context) fn
      | Add_predecessor_block_metadata_hash of (context * hash, context) fn
      | Add_predecessor_ops_metadata_hash of (context * hash, context) fn
      | Add_test_chain of (context * test_chain_status, context) fn
      | Remove_test_chain of (context, context) fn
      | Fork_test_chain of (context * hash * time_protocol, context) fn
      (* [__ _o i__] *)
      | Checkout of (commit_hash_lhs, context) fn
      (* | Checkout_exn of (commit_hash, context) fn *)
      (* [__ __ i_m] *)
      (* | Close *)
      (* | Sync *)
      (* | Set_master of (commit_hash, unit) fn *)
      (* | Set_head of (chain_id * commit_hash, unit) fn *)
      | Commit_genesis of (chain_id * time_protocol * hash, commit_hash_rhs) fn
      | Clear_test_chain of (chain_id, unit) fn
      (* [__ i_ __m] *)
      | Commit of (time_protocol * message option * context, commit_hash_rhs) fn
      (* [__ ~~ _o_] *)
      | Init of (bool option, unit) fn
      | Patch_context_enter of context
      | Patch_context_exit of context * context
    (* * block_info option *)
    (* | Commit_test_chain_genesis of
     *     ( context * (time_protocol * block_level * hash),
     *       unit (\* output not recorded *\) )
     *     fn *)
    [@@deriving repr]

    type row = {
      level : int;
      op_count : int;
      op_count_tx : int;
      op_count_contract : int;
      ops : event array;
      uses_patch_context : bool;
    }
    [@@deriving repr]
    (** Events of a block. The first/last are either init/commit_genesis or
        checkout(exn)/commit. *)

    type header = {
      initial_block : (block_level * hash) option;
      last_block : block_level * hash;
      block_count : int;
    }
    [@@deriving repr]
  end

  module Latest = V1
  include Latest

  include Trace_common.Io (struct
    module Latest = Latest

    (** Irmin's Replayable Bootstrap Trace *)
    let magic = Trace_common.Magic.of_string "IrmRepBT"

    let get_version_converter = function
      | 0 -> failwith "replayable actions trace v0 are deprecated"
      | 1 ->
          Trace_common.Version_converter
            {
              header_t = V1.header_t;
              row_t = V1.row_t;
              upgrade_header = Fun.id;
              upgrade_row = Fun.id;
            }
      | i -> Fmt.invalid_arg "Unknown replayable actions trace version %d" i
  end)
end

(** Trace of a tezos node run, or a replay run.

    May be summarised to a JSON file.

    {3 Implicitly Auto-Upgradable File Format}

    The stat trace has these two properties:

    - It supports extensions, in order to change or add new stats in the future.
    - Old trace files from old versions are still readable.

    There are multiple reasons for wanting compatibility with old versions:

    - Because one of the goal of the benchmarks is to assess the evolution of
      performances across distant versions of irmin, we need stability in order
      to avoid recomputing everything every time.
    - When those traces will be produced by Tezos nodes, we have no control over
      the version of those traces.

    For this system to work, the "decoding shape" of a version of the stat trace
    shouldn't ever change (once fixed). The way the trace is built for a version
    should be stable too.

    To modify something in the definition or the collection: append a new
    version. *)
module Stat_trace = struct
  module V0 = struct
    type float32 = int32 [@@deriving repr]

    let version = 0

    type pack = {
      finds : int;
      cache_misses : int;
      appended_hashes : int;
      appended_offsets : int;
    }
    [@@deriving repr]
    (** Stats extracted from [Irmin_pack.Stats.get ()]. *)

    type tree = {
      contents_hash : int;
      contents_find : int;
      contents_add : int;
      node_hash : int;
      node_mem : int;
      node_add : int;
      node_find : int;
      node_val_v : int;
      node_val_find : int;
      node_val_list : int;
    }
    [@@deriving repr]
    (** Stats extracted from [Irmin.Tree.counters ()]. *)

    type index = {
      bytes_read : int;
      nb_reads : int;
      bytes_written : int;
      nb_writes : int;
      nb_merge : int;
      new_merge_durations : float list;
    }
    [@@deriving repr]
    (** Stats extracted from [Index.Stats.get ()].

        [new_merge_durations] is not just a mirror of
        [Index.Stats.merge_durations], it only contains the new entries since
        the last time it was recorded. This list is always empty when in the
        header. *)

    type gc = {
      minor_words : float;
      promoted_words : float;
      major_words : float;
      minor_collections : int;
      major_collections : int;
      heap_words : int;
      compactions : int;
      top_heap_words : int;
      stack_size : int;
    }
    [@@deriving repr]
    (** Stats extracted from [Gc.quick_stat ()]. *)

    type disk = {
      index_data : int64;
      index_log : int64;
      index_log_async : int64;
      store_dict : int64;
      store_pack : int64;
    }
    [@@deriving repr]
    (** Stats extracted from filesystem. Requires the path to the irmin store. *)

    type bag_of_stats = {
      pack : pack;
      tree : tree;
      index : index;
      gc : gc;
      disk : disk;
      timestamp_wall : float;
      timestamp_cpu : float;
    }
    [@@deriving repr]
    (** Melting pot of stats, recorded before and after every commits.

        They are necessary in order to compute any throughput analytics. *)

    type store_before = {
      nodes : int;
      leafs : int;
      skips : int;
      depth : int;
      width : int;
    }
    [@@deriving repr]
    (** Stats computed from the [tree] value passed to the commit operation,
        before the commit, when the tree still carries the modifications brought
        by the previous operations. *)

    type watched_node =
      [ `Contracts_index
      | `Big_maps_index
      | `Rolls_index
      | `Rolls_owner_current
      | `Commitments
      | `Contracts_index_ed25519
      | `Contracts_index_originated ]
    [@@deriving repr, enum]

    type store_after = { watched_nodes_length : int list } [@@deriving repr]
    (** Stats computed on the [tree] value passed to the commit operation, after
        the commit, when the inode has been reconstructed and that [Tree.length]
        is now innexpensive to perform. *)

    type commit = {
      duration : float32;
      before : bag_of_stats;
      after : bag_of_stats;
      store_before : store_before;
      store_after : store_after;
    }
    [@@deriving repr]

    type row =
      [ `Add of float32
      | `Remove of float32
      | `Find of float32
      | `Mem of float32
      | `Mem_tree of float32
      | `Checkout of float32
      | `Copy of float32
      | `Commit of commit ]
    [@@deriving repr]
    (** Stats gathered while running an operation.

        {3 Operation durations}

        For each operation we record its wall time length using a [float32], a
        [float16] would be suitable too (it has >3 digits of precision).

        {3 Time and disk performance considerations}

        On commit we record a lot of things, thankfuly the frequency is low:
        ~1/600. 599 small operations weigh ~3600 bytes, 1 commit weighs ~300
        bytes. The trace reaches 1GB after ~250k commits. *)

    type setup_play = unit [@@deriving repr]
    (** Informations gathered from the tezos node.

        Noting so far. Any ideas? *)

    type setup_replay = {
      path_conversion : [ `None | `V1 | `V0_and_v1 | `V0 ];
      artefacts_dir : string;
    }
    [@@deriving repr]
    (** Informations gathered from the tree.exe parameters. *)

    type config = {
      inode_config : int * int * int;
      store_type : [ `Pack | `Pack_layered | `Pack_mem ];
      setup : [ `Play of setup_play | `Replay of setup_replay ];
    }
    [@@deriving repr]

    type header = {
      config : config;
      hostname : string;
      timeofday : float;
      word_size : int;
      initial_stats : bag_of_stats;
    }
    [@@deriving repr]
    (** File header.

        {3 Timestamps}

        [stats.timestamp_wall] and [stats.timestamp_cpu] are the starting points
        of the trace, they are to be substracted from their counterpart in
        [commit] to compute time spans.

        [timeofday] is the date and time at which the stats started to be
        accumulated.

        [stats.timestamp_wall] may originate from [Mtime_clock.now].

        [stats.timestamp_cpu] may originate from [Sys.time].

        It would be great to be able to record the library/sources versions. *)
  end

  module Latest = V0
  include Latest

  let watched_nodes : watched_node list =
    List.init (max_watched_node + 1) (fun i ->
        watched_node_of_enum i |> Option.get)

  let step_list_per_watched_node =
    let aux = function
      | `Contracts_index -> [ "data"; "contracts"; "index" ]
      | `Big_maps_index -> [ "data"; "big_maps"; "index" ]
      | `Rolls_index -> [ "data"; "rolls"; "index" ]
      | `Rolls_owner_current -> [ "data"; "rolls"; "owner"; "current" ]
      | `Commitments -> [ "data"; "commitments" ]
      | `Lol -> []
      | `Contracts_index_ed25519 -> [ "data"; "contracts"; "index"; "ed25519" ]
      | `Contracts_index_originated ->
          [ "data"; "contracts"; "index"; "originated" ]
    in
    List.combine watched_nodes (List.map aux watched_nodes)

  let path_per_watched_node =
    List.map
      (fun (k, l) -> (k, "/" ^ String.concat "/" l))
      step_list_per_watched_node

  include Trace_common.Io (struct
    module Latest = Latest

    (** Irmin's Stats Bootstrap Trace *)
    let magic = Trace_common.Magic.of_string "IrmStaBT"

    let get_version_converter = function
      | 0 ->
          Trace_common.Version_converter
            {
              header_t = V0.header_t;
              row_t = V0.row_t;
              upgrade_header = Fun.id;
              upgrade_row = Fun.id;
            }
      | i -> Fmt.invalid_arg "Unknown Stat_trace version %d" i
  end)
end
