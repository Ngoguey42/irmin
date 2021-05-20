open Irmin.Export_for_backends
module Def0 = Trace_definitions.Raw_actions_trace
module Def1 = Trace_definitions.Replayable_actions_trace
module Utils = Trace_stat_summary_utils

(** An [Iterator.t] is a non-empty [Seq.t] where the head is accessible and the
    tail is functional. *)
module Iterator : sig
  type 'a t

  val create : 'a Seq.t -> 'a t option
  val head : 'a t -> 'a
  val tail : 'a t -> 'a t option
end = struct
  type 'a t = { head : 'a; tail : 'a Seq.node Lazy.t }

  let create seq =
    match seq () with
    | Seq.Nil -> None
    | Cons (head, tail) -> Some { head; tail = Lazy.from_fun tail }

  let head { head; _ } = head

  let tail { tail; _ } =
    match Lazy.force tail with
    | Seq.Nil -> None
    | Cons (head, tail) -> Some { head; tail = Lazy.from_fun tail }
end

let convolve_with_padding :
    ('a option -> 'a -> 'a option -> 'b) -> 'a list -> 'b list =
 fun f l ->
  let a = Array.of_list l in
  let count = Array.length a in
  List.init (Array.length a) (fun i ->
      let prev = if i = 0 then None else Some a.(i - 1) in
      let next = if i = count - 1 then None else Some a.(i + 1) in
      f prev a.(i) next)

let iter_2by2 : ('a -> 'a -> unit) -> 'a list -> unit =
 fun f l ->
  let a = Array.of_list l in
  let count = Array.length a in
  for i = 0 to count - 2 do
    f a.(i) a.(i + 1)
  done

type hash = Def0.hash

type tracker_id_tables = {
  occurrence_count_per_tree_id : (Def0.tracker, int) Hashtbl.t;
  occurrence_count_per_context_id : (Def0.tracker, int) Hashtbl.t;
  occurrence_count_per_commit_hash : (string, int * int) Hashtbl.t;
}
(** The 3 massive hash tables that will contain all the tracker occurences from
    the trace. *)

type event_details = {
  rank : [ `Ignore | `Crash | `Use | `Control_flow ];
  tracker_ids : Def0.tracker list * Def0.tracker list * Def0.commit_hash list;
  to_v1 : tracker_id_tables -> Def1.event;
}
(** A summary of a [Def0.row] *)

let v1_of_v0_test_chain_status = function
  | Def0.Not_running -> Def1.Not_running
  | Forking { protocol; expiration } -> Def1.(Forking { protocol; expiration })
  | Running { chain_id; genesis; protocol; expiration } ->
      Def1.(Running { chain_id; genesis; protocol; expiration })

(** A single large multi-purpose pattern matching to filter/inspect/map the raw
    rows. *)
let event_infos =
  let ignore_ =
    {
      rank = `Ignore;
      tracker_ids = ([], [], []);
      to_v1 = (fun _ -> assert false);
    }
  in
  let crash =
    {
      rank = `Crash;
      tracker_ids = ([], [], []);
      to_v1 = (fun _ -> assert false);
    }
  in
  let to_tree t = Def1.Tree t in

  let scopec, scopet =
    let add_scope tbl k =
      match Hashtbl.find_opt tbl k with
      | Some 0 | None -> Fmt.failwith "Unexpected scope"
      | Some 1 ->
          Hashtbl.remove tbl k;
          (Def1.Last_occurence, k)
      | Some i ->
          Hashtbl.replace tbl k (i - 1);
          (Def1.Will_reoccur, k)
    in
    ( (fun ids -> add_scope ids.occurrence_count_per_context_id),
      fun ids -> add_scope ids.occurrence_count_per_tree_id )
  in
  let scopeh_rhs ids k =
    let tbl = ids.occurrence_count_per_commit_hash in
    match Hashtbl.find_opt tbl k with
    | Some (0, _) | None -> Fmt.failwith "Unexpected scope"
    | Some (remaining, instanciations_before) ->
        let scope_start =
          if instanciations_before = 0 then Def1.First_instanciation
          else Def1.Reinstanciation
        in
        let scope_end =
          if remaining = 1 then Def1.Last_occurence else Def1.Will_reoccur
        in
        if remaining = 1 then Hashtbl.remove tbl k
        else Hashtbl.replace tbl k (remaining - 1, instanciations_before + 1);
        (scope_start, scope_end, k)
  in
  let scopeh_lhs ids k =
    let tbl = ids.occurrence_count_per_commit_hash in
    match Hashtbl.find_opt tbl k with
    | Some (0, _) | None -> Fmt.failwith "Unexpected scope"
    | Some (remaining, instanciations_before) ->
        let scope_start =
          if instanciations_before = 0 then Def1.Not_instanciated
          else Def1.Instanciated
        in
        let scope_end =
          if remaining = 1 then Def1.Last_occurence else Def1.Will_reoccur
        in
        if remaining = 1 then Hashtbl.remove tbl k
        else Hashtbl.replace tbl k (remaining - 1, instanciations_before);
        (scope_start, scope_end, k)
  in
  let open Def0 in
  function
  | Def0.Tree v -> (
      let open Tree in
      match v with
      | Empty (x, t) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Empty (x, scopet ids t) |> to_tree);
          }
      | Of_raw (x, t) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Of_raw (x, scopet ids t) |> to_tree);
          }
      | Of_value (x, t) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Of_value (x, scopet ids t) |> to_tree);
          }
      | Mem ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Mem ((scopet ids t, x), y) |> to_tree);
          }
      | Mem_tree ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 =
              (fun ids -> Def1.Tree.Mem_tree ((scopet ids t, x), y) |> to_tree);
          }
      | Find ((t, x), y) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Find ((scopet ids t, x), Option.is_some y) |> to_tree);
          }
      | Is_empty (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Is_empty (scopet ids t, x) |> to_tree);
          }
      | Kind (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Kind (scopet ids t, x) |> to_tree);
          }
      | Hash (t, _) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 = (fun ids -> Def1.Tree.Hash (scopet ids t, ()) |> to_tree);
          }
      | Equal ((t, t'), x) ->
          {
            rank = `Use;
            tracker_ids = ([ t; t' ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Equal ((scopet ids t, scopet ids t'), x) |> to_tree);
          }
      | To_value (t, x) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.To_value (scopet ids t, Option.is_some x) |> to_tree);
          }
      | Clear ((x, t), ()) ->
          {
            rank = `Use;
            tracker_ids = ([ t ], [], []);
            to_v1 =
              (fun ids -> Def1.Tree.Clear ((x, scopet ids t), ()) |> to_tree);
          }
      | Find_tree ((t, x), t') ->
          {
            rank = `Use;
            tracker_ids = ([ t ] @ Option.to_list t', [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Find_tree
                  ((scopet ids t, x), Option.map (scopet ids) t')
                |> to_tree);
          }
      | Add ((t, x, y), t') ->
          {
            rank = `Use;
            tracker_ids = ([ t; t' ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Add ((scopet ids t, x, y), scopet ids t') |> to_tree);
          }
      | Add_tree ((t, x, t'), t'') ->
          {
            rank = `Use;
            tracker_ids = ([ t; t'; t'' ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Add_tree
                  ((scopet ids t, x, scopet ids t'), scopet ids t'')
                |> to_tree);
          }
      | Remove ((t, x), t') ->
          {
            rank = `Use;
            tracker_ids = ([ t; t' ], [], []);
            to_v1 =
              (fun ids ->
                Def1.Tree.Remove ((scopet ids t, x), scopet ids t') |> to_tree);
          }
      | Shallow -> crash
      | To_raw -> ignore_
      | Pp -> ignore_
      | List _ -> (* see List for rationale *) crash
      | Fold_start _ -> (* see List for rationale *) crash
      | Fold_step_enter _ -> crash
      | Fold_step_exit _ -> crash
      | Fold_end _ -> crash)
  | Find_tree ((c, x), t') ->
      {
        rank = `Use;
        tracker_ids = (Option.to_list t', [ c ], []);
        to_v1 =
          (fun ids ->
            Def1.Find_tree ((scopec ids c, x), Option.map (scopet ids) t'));
      }
  | Fold_start (x, c, y) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Fold_start (x, scopec ids c, y));
      }
  | Fold_step_enter t ->
      {
        rank = `Use;
        tracker_ids = ([ t ], [], []);
        to_v1 = (fun ids -> Def1.Fold_step_enter (scopet ids t));
      }
  | Fold_step_exit t ->
      {
        rank = `Use;
        tracker_ids = ([ t ], [], []);
        to_v1 = (fun ids -> Def1.Fold_step_exit (scopet ids t));
      }
  | Fold_end _ ->
      {
        rank = `Use;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Fold_end);
      }
  | Add_tree ((c, x, t'), t'') ->
      {
        rank = `Use;
        tracker_ids = ([ t'; t'' ], [ c ], []);
        to_v1 =
          (fun ids ->
            Def1.Add_tree ((scopec ids c, x, scopet ids t'), scopet ids t''));
      }
  | Mem ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Mem ((scopec ids c, x), y));
      }
  | Mem_tree ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Mem_tree ((scopec ids c, x), y));
      }
  | Find ((c, x), y) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Find ((scopec ids c, x), Option.is_some y));
      }
  | Get_protocol (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Get_protocol (scopec ids c, ()));
      }
  | Hash ((x, y, c), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Hash ((x, y, scopec ids c), ()));
      }
  | Find_predecessor_block_metadata_hash (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 =
          (fun ids ->
            Def1.Find_predecessor_block_metadata_hash (scopec ids c, ()));
      }
  | Find_predecessor_ops_metadata_hash (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 =
          (fun ids ->
            Def1.Find_predecessor_ops_metadata_hash (scopec ids c, ()));
      }
  | Get_test_chain (c, _) ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Get_test_chain (scopec ids c, ()));
      }
  | Exists ((h, x), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [], [ h ]);
        to_v1 = (fun ids -> Def1.Exists (scopeh_lhs ids h, x));
      }
  | Retrieve_commit_info ((h, x), _) ->
      {
        rank = `Use;
        tracker_ids = ([], [], [ h ]);
        to_v1 = (fun ids -> Def1.Exists (scopeh_lhs ids h, x));
      }
  | Add ((c, x, y), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 = (fun ids -> Def1.Add ((scopec ids c, x, y), scopec ids c'));
      }
  | Remove ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 = (fun ids -> Def1.Remove ((scopec ids c, x), scopec ids c'));
      }
  | Add_protocol ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids -> Def1.Add_protocol ((scopec ids c, x), scopec ids c'));
      }
  | Add_predecessor_block_metadata_hash ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids ->
            Def1.Add_predecessor_block_metadata_hash
              ((scopec ids c, x), scopec ids c'));
      }
  | Add_predecessor_ops_metadata_hash ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids ->
            Def1.Add_predecessor_ops_metadata_hash
              ((scopec ids c, x), scopec ids c'));
      }
  | Add_test_chain ((c, x), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids ->
            Def1.Add_test_chain
              ((scopec ids c, v1_of_v0_test_chain_status x), scopec ids c'));
      }
  | Remove_test_chain (c, c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids -> Def1.Remove_test_chain (scopec ids c, scopec ids c'));
      }
  | Fork_test_chain ((c, x, y), c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids ->
            Def1.Fork_test_chain ((scopec ids c, x, y), scopec ids c'));
      }
  | Checkout ((h, Some c), _) | Checkout_exn ((h, Ok c), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [ c ], [ h ]);
        to_v1 = (fun ids -> Def1.Checkout (scopeh_lhs ids h, scopec ids c));
      }
  | Commit_genesis ((params, Ok h), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [], [ h ]);
        to_v1 = (fun ids -> Def1.Commit_genesis (params, scopeh_rhs ids h));
      }
  | Clear_test_chain (x, ()) ->
      {
        rank = `Use;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Clear_test_chain (x, ()));
      }
  | Commit (((x, y, c), h), _) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [ c ], [ h ]);
        to_v1 =
          (fun ids -> Def1.Commit ((x, y, scopec ids c), scopeh_rhs ids h));
      }
  | Init (ro, ()) ->
      {
        rank = `Control_flow;
        tracker_ids = ([], [], []);
        to_v1 = (fun _ -> Def1.Init (ro, ()));
      }
  | Patch_context_enter c ->
      {
        rank = `Use;
        tracker_ids = ([], [ c ], []);
        to_v1 = (fun ids -> Def1.Patch_context_enter (scopec ids c));
      }
  | Patch_context_exit (c, Ok c') ->
      {
        rank = `Use;
        tracker_ids = ([], [ c; c' ], []);
        to_v1 =
          (fun ids -> Def1.Patch_context_exit (scopec ids c, scopec ids c'));
      }
  | List _ ->
      (* Since there are no occurences of it in the Raw trace, and since this is
         tricky to implement, let's not implement it yet to avoid introducing
         non-covered bugs *)
      crash
  | Merkle_tree _ ->
      (* No occurences in raw and missing types from opam's tezos libs *)
      ignore_
  | Restore_context -> crash
  | Restore_integrity -> crash
  | Dump_context -> ignore_
  | Check_protocol_commit_consistency -> ignore_
  | Validate_context_hash_consistency_and_commit -> ignore_
  | Commit_test_chain_genesis _ ->
      (* No occurences in raw but would requires a special handling in the stats *)
      crash
  | Checkout ((_, None), _)
  | Checkout_exn ((_, Error _), _)
  | Commit_genesis ((_, Error _), _)
  | Patch_context_exit (_, Error _) ->
      (* Let's not handle these failed calls *)
      crash
  | Close | Sync _ | Set_master _ | Set_head _ -> crash

module Pass0 = struct
  module Op_count = struct
    type t = { ignored : int; used : int }

    let folder =
      let acc0 = { ignored = 0; used = 0 } in
      let accumulate acc ((_ : int64), row) =
        match event_infos row with
        | { rank = `Ignore; _ } -> { acc with ignored = acc.ignored + 1 }
        | _ -> { acc with used = acc.used + 1 }
      in
      let finalise = Fun.id in
      Utils.Parallel_folders.folder acc0 accumulate finalise
  end

  module Id_counts = struct
    type t = tracker_id_tables

    let folder =
      let acc0 =
        {
          occurrence_count_per_tree_id = Hashtbl.create 100_000_000;
          occurrence_count_per_context_id = Hashtbl.create 100_000_000;
          occurrence_count_per_commit_hash = Hashtbl.create 1_000_000;
        }
      in
      let accumulate acc ((i : int64), row) =
        if Int64.rem i 25_000_000L = 0L then
          Printf.eprintf "Pass0 - Dealing with row idx %#Ld\n%!" i;
        match event_infos row with
        | { rank = `Use; tracker_ids; _ }
        | { rank = `Control_flow; tracker_ids; _ } ->
            let tree_ids, context_ids, commit_hashes = tracker_ids in
            let incr tbl k =
              match Hashtbl.find_opt tbl k with
              | None -> Hashtbl.add tbl k 1
              | Some i -> Hashtbl.replace tbl k (i + 1)
            in
            List.iter (incr acc.occurrence_count_per_tree_id) tree_ids;
            List.iter (incr acc.occurrence_count_per_context_id) context_ids;
            let incr tbl k =
              match Hashtbl.find_opt tbl k with
              | None -> Hashtbl.add tbl k (1, 0)
              | Some (i, _) -> Hashtbl.replace tbl k (i + 1, 0)
            in
            List.iter (incr acc.occurrence_count_per_commit_hash) commit_hashes;
            acc
        | _ -> acc
      in
      let finalise = Fun.id in
      Utils.Parallel_folders.folder acc0 accumulate finalise
  end

  module Segmentation = struct
    let level_of_commit_message message =
      match
        String.split_on_char ',' message
        |> List.map String.trim
        |> List.map (String.split_on_char ' ')
      with
      | [ [ "lvl"; lvl ]; [ "fit"; _ ]; [ "prio"; _ ]; [ _; "ops" ] ] ->
          int_of_string lvl
      | _ -> Fmt.failwith "Could not parse commit message: `%s`" message

    type block_summary = {
      first_row_idx : int64;
      last_row_idx : int64;
      checkout_hash : hash option;
      uses_patch_context : bool;
      commit_hash : hash;
      level : int option;
    }

    type acc = {
      ingest_row : acc -> int64 * Def0.row -> acc;
      summary_per_block_rev : block_summary list;
    }

    type t = block_summary list

    let rec look_for_block_start acc (idx, row) =
      let open Def0 in
      match row with
      | Init (_, _) -> { acc with ingest_row = look_after_init idx }
      | Checkout ((h, Some _), _) | Checkout_exn ((h, Ok _), _) ->
          { acc with ingest_row = look_for_commit idx h }
      | row ->
          Fmt.failwith "Unexpected op at the beginning of a block: %a"
            (Repr.pp Def0.row_t) row

    and look_after_init first_row_idx acc (_, row) =
      let open Def0 in
      match (event_infos row, row) with
      | _, (Checkout ((h, Some _), _) | Checkout_exn ((h, Ok _), _)) ->
          { acc with ingest_row = look_for_commit first_row_idx h }
      | { rank = `Use; _ }, _ ->
          { acc with ingest_row = look_for_commit_genesis first_row_idx false }
      | { rank = `Control_flow; _ }, _
      | { rank = `Ignore; _ }, _
      | { rank = `Crash; _ }, _ ->
          Fmt.failwith "Can't handle the following raw event after an init: %a"
            (Repr.pp Def0.row_t) row

    and look_for_commit_genesis first_row_idx uses_patch_context acc (idx, row)
        =
      let open Def0 in
      match (event_infos row, row) with
      | { rank = `Ignore; _ }, _ -> acc
      | _, Patch_context_enter _ ->
          assert (not uses_patch_context);
          { acc with ingest_row = look_for_commit_genesis first_row_idx true }
      | { rank = `Use; _ }, _ -> acc
      | { rank = `Control_flow; _ }, Commit_genesis ((_, Ok h), _) ->
          let block =
            {
              first_row_idx;
              last_row_idx = idx;
              level = Some 0;
              checkout_hash = None;
              commit_hash = h;
              uses_patch_context;
            }
          in
          {
            ingest_row = look_for_block_start;
            summary_per_block_rev = block :: acc.summary_per_block_rev;
          }
      | { rank = `Crash; _ }, _ | { rank = `Control_flow; _ }, _ ->
          Fmt.failwith "Can't handle the following raw event: %a"
            (Repr.pp Def0.row_t) row Fmt.failwith
            "Unexpected op at the end of a block: %a" (Repr.pp Def0.row_t) row

    and look_for_commit first_row_idx checkout_hash acc (idx, row) =
      let open Def0 in
      match (event_infos row, row) with
      | { rank = `Ignore; _ }, _ -> acc
      | _, Commit (((_, m, _), h), _) ->
          let lvl = Option.map level_of_commit_message m in
          let block =
            {
              first_row_idx;
              last_row_idx = idx;
              level = lvl;
              checkout_hash = Some checkout_hash;
              commit_hash = h;
              uses_patch_context = false;
            }
          in
          {
            ingest_row = look_for_block_start;
            summary_per_block_rev = block :: acc.summary_per_block_rev;
          }
      | { rank = `Control_flow; _ }, _
      | _, Patch_context_enter _
      | { rank = `Crash; _ }, _ ->
          Fmt.failwith "Can't handle the following raw event: %a"
            (Repr.pp Def0.row_t) row
      | { rank = `Use; _ }, _ -> acc

    let folder =
      let acc0 =
        { ingest_row = look_for_block_start; summary_per_block_rev = [] }
      in
      let accumulate acc row = acc.ingest_row acc row in
      let finalise { summary_per_block_rev; _ } =
        List.rev summary_per_block_rev
      in
      Utils.Parallel_folders.folder acc0 accumulate finalise
  end
end

module Pass1 = struct
  type block_summary = {
    first_row_idx : int64;
    last_row_idx : int64;
    checkout_hash : hash option;
    commit_hash : hash;
    uses_patch_context : bool;
    level : int;
  }

  let check_result l =
    List.iter (fun i -> if i < 0 then failwith "Illegal negative block level") l;
    iter_2by2
      (fun a b ->
        match (a = b, a + 1 = b) with
        | false, false ->
            failwith
              "Consecutive blocks should have an increasing or equal level."
        | true, _ | _, true -> ())
      l

  let interpolate_block_ids pass0_seg =
    let l =
      let open Pass0.Segmentation in
      List.rev_map
        (function
          | { level = Some level; _ } -> `Present level
          | { level = None; _ } -> `Missing)
        pass0_seg
      |> List.rev
    in
    let l =
      let map_middle prev_opt v next_opt =
        match (prev_opt, v, next_opt) with
        | _, `Present v, _ -> v
        | None, `Missing, None ->
            failwith "Can't interpolate block level when only 1 missing block"
        | None, `Missing, Some (`Missing | `Present _) ->
            failwith "Can't interpolate block level when first is absent"
        | Some (`Missing | `Present _), `Missing, None ->
            failwith "Can't interpolate block level when last is absent"
        | Some `Missing, `Missing, _ | _, `Missing, Some `Missing ->
            failwith "Can't interpolate block level when 2 in a row absent"
        | Some (`Present prev), `Missing, Some (`Present next) ->
            if prev + 2 = next then prev + 1
            else if prev + 1 = next then
              failwith "Can't interpolate orphan block level"
            else failwith "Unexpected block levels"
      in
      convolve_with_padding map_middle l
    in
    check_result l;
    List.rev_map2
      (fun Pass0.Segmentation.
             {
               first_row_idx;
               last_row_idx;
               checkout_hash;
               commit_hash;
               uses_patch_context;
               _;
             } level ->
        {
          level;
          first_row_idx;
          last_row_idx;
          checkout_hash;
          commit_hash;
          uses_patch_context;
        })
      pass0_seg l
    |> List.rev
end

module Pass2 = struct
  let rec run ids row_seq writer block_summaries =
    match block_summaries with
    | [] -> Lwt.return_unit
    | Pass1.{ last_row_idx; level; uses_patch_context; _ } :: tl ->
        if level mod 20_000 = 0 then
          Printf.eprintf "Pass2 - Dealing with block lvl %#d\n%!" level;
        let row_seq, rows =
          Utils.Seq.take_up_to ~is_last:(fun (i, _) -> i = last_row_idx) row_seq
        in
        let ops =
          rows
          |> List.filter_map (fun (_, row) ->
                 match event_infos row with
                 | { rank = `Ignore; _ } -> None
                 | { to_v1; _ } -> Some (to_v1 ids))
          |> Array.of_list
        in
        let* Tezos_history_metrics.
               { op_count; op_count_tx; op_count_contract; _ } =
          (* The current commit might actually be an orphan block. The number
             of operations will be wrong in that case. *)
          Utils.operations_of_block_level level
        in
        let row =
          Def1.
            {
              level;
              ops;
              op_count;
              op_count_tx;
              op_count_contract;
              uses_patch_context;
            }
        in
        Def1.append_row writer row;
        run ids row_seq writer tl
end

(* let () =
 *   let seg =
 *     List.init 1_000_000 (fun i ->
 *         Pass0.Segmentation.
 *           {
 *             first_row_idx = Int64.of_int i;
 *             last_row_idx = Int64.of_int i;
 *             checkout_hash = Some (string_of_int (i - 1));
 *             commit_hash = string_of_int i;
 *             level = Some i;
 *           })
 *   in
 *   Printf.eprintf "Pass1\n%!";
 *   let seg = Pass1.interpolate_block_ids seg in
 *   let () =
 *     let count = List.length seg in
 *     Printf.eprintf "Pass1 done. \n  First/last block levels are %#d/%#d \n%!"
 *       (if count = 0 then 0 else (List.hd seg).level)
 *       (if count = 0 then 0 else (List.nth seg (count - 1)).level)
 *   in
 *   () *)

let run in_path out_chan =
  (* Choose the right file in the raw trace directory *)
  let in_paths = Def0.trace_files_of_trace_directory ~filter:[ `Rw ] in_path in
  if List.length in_paths <> 1 then
    Fmt.failwith "Expecting exactly one RW trace file in %s. Got %d" in_path
      (List.length in_paths);
  let in_path, _pid = List.hd in_paths in
  Printf.eprintf "Using %s\n%!" in_path;

  (* Pass 0, segment blocks, summarise tracker ids *)
  let op_counts, ids, seg =
    let construct op_counts ids seg = (op_counts, ids, seg) in
    let pf0 =
      let open Utils.Parallel_folders in
      open_ construct
      |+ Pass0.Op_count.folder
      |+ Pass0.Id_counts.folder
      |+ Pass0.Segmentation.folder
      |> seal
    in
    Def0.open_reader in_path
    |> snd
    |> Utils.Seq.mapi64

    |> Utils.Seq.take_up_to ~is_last:(fun (i, _) -> i < 5000L) |> snd |> List.to_seq

    |> Seq.fold_left Utils.Parallel_folders.accumulate pf0
    |> Utils.Parallel_folders.finalise
  in
  let () =
    let count = List.length seg in
    Printf.eprintf
      "Pass0 done. \n\
      \  Found %#d blocks, %#d trees, %#d contexts, %#d commit hashes. \n\
      \  %#d operations ignored and %#d operations used. \n\
       %!"
      count
      (Hashtbl.length ids.occurrence_count_per_tree_id)
      (Hashtbl.length ids.occurrence_count_per_context_id)
      (Hashtbl.length ids.occurrence_count_per_commit_hash)
      op_counts.Pass0.Op_count.ignored op_counts.Pass0.Op_count.used
  in

  (* Pass 1, fill the holes in segmentation *)
  Printf.eprintf "Pass1\n%!";
  let seg = Pass1.interpolate_block_ids seg in
  let () =
    let count = List.length seg in
    Printf.eprintf "Pass1 done. \n  First/last block levels are %#d/%#d \n%!"
      (if count = 0 then 0 else (List.hd seg).level)
      (if count = 0 then 0 else (List.nth seg (count - 1)).level)
  in

  (* Pass 2, write to destination channel *)
  let reader = Def0.open_reader in_path |> snd |> Utils.Seq.mapi64 in
  let header =
    let block_count = List.length seg in
    let initial_block =
      let record = List.hd seg in
      match record.Pass1.checkout_hash with
      | None -> None
      | Some h -> Some (record.Pass1.level, h)
    in
    let last_block =
      let record = List.nth seg (block_count - 1) in
      Pass1.(record.level, record.commit_hash)
    in
    Def1.{ block_count; initial_block; last_block }
  in
  let writer = Def1.create out_chan header in
  Pass2.run ids reader writer seg
