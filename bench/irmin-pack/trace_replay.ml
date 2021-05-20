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

open Irmin.Export_for_backends
open Bench_common
include Trace_replay_intf
module Def = Trace_definitions.Replayable_actions_trace
module Context_hash = Tezos_base.TzPervasives.Context_hash

let should_check_hashes config =
  config.path_conversion = `None
  && config.inode_config = (32, 256)
  && config.empty_blobs = false

let is_hex_char = function
  | '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' -> true
  | _ -> false

let is_2char_hex s =
  if String.length s <> 2 then false
  else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char

let all_6_2char_hex a b c d e f =
  is_2char_hex a
  && is_2char_hex b
  && is_2char_hex c
  && is_2char_hex d
  && is_2char_hex e
  && is_2char_hex f

let is_30char_hex s =
  if String.length s <> 30 then false
  else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char

(** This function flattens all the 6 step-long chunks forming 40 byte-long
    hashes to a single step.

    Those flattenings are performed during the trace replay, i.e. they count in
    the total time.

    If a path contains 2 or more of those patterns, only the leftmost one is
    converted.

    A chopped hash has this form

    {v ([0-9a-f]{2}/){5}[0-9a-f]{30} v}

    and is flattened to that form

    {v [0-9a-f]{40} v} *)
let flatten_v0 key =
  let rec aux rev_prefix suffix =
    match suffix with
    | a :: b :: c :: d :: e :: f :: tl
      when is_2char_hex a
           && is_2char_hex b
           && is_2char_hex c
           && is_2char_hex d
           && is_2char_hex e
           && is_30char_hex f ->
        let mid = a ^ b ^ c ^ d ^ e ^ f in
        aux (mid :: rev_prefix) tl
    | hd :: tl -> aux (hd :: rev_prefix) tl
    | [] -> List.rev rev_prefix
  in
  aux [] key

(** This function removes from the paths all the 6 step-long hashes of this form

    {v ([0-9a-f]{2}/){6} v}

    Those flattenings are performed during the trace replay, i.e. they count in
    the total time.

    The paths in tezos:
    https://www.dailambda.jp/blog/2020-05-11-plebeia/#tezos-path

    Tezos' PR introducing this flattening:
    https://gitlab.com/tezos/tezos/-/merge_requests/2771 *)
let flatten_v1 = function
  | "data" :: "contracts" :: "index" :: a :: b :: c :: d :: e :: f :: tl
    when all_6_2char_hex a b c d e f -> (
      match tl with
      | hd :: "delegated" :: a :: b :: c :: d :: e :: f :: tl
        when all_6_2char_hex a b c d e f ->
          "data" :: "contracts" :: "index" :: hd :: "delegated" :: tl
      | _ -> "data" :: "contracts" :: "index" :: tl)
  | "data" :: "big_maps" :: "index" :: a :: b :: c :: d :: e :: f :: tl
    when all_6_2char_hex a b c d e f ->
      "data" :: "big_maps" :: "index" :: tl
  | "data" :: "rolls" :: "index" :: _ :: _ :: tl ->
      "data" :: "rolls" :: "index" :: tl
  | "data" :: "rolls" :: "owner" :: "current" :: _ :: _ :: tl ->
      "data" :: "rolls" :: "owner" :: "current" :: tl
  | "data" :: "rolls" :: "owner" :: "snapshot" :: a :: b :: _ :: _ :: tl ->
      "data" :: "rolls" :: "owner" :: "snapshot" :: a :: b :: tl
  | l -> l

(* let flatten_op ~flatten_path = function
 *   (\* | _ -> failwith "super" *\)
 *   | Def.Checkout _ as op -> op
 *   | Add op -> Add { op with key = flatten_path op.key }
 *   | Remove (keys, in_ctx_id, out_ctx_id) ->
 *       Remove (flatten_path keys, in_ctx_id, out_ctx_id)
 *   | Copy op ->
 *       Copy
 *         {
 *           op with
 *           key_src = flatten_path op.key_src;
 *           key_dst = flatten_path op.key_dst;
 *         }
 *   | Find (keys, b, ctx) -> Find (flatten_path keys, b, ctx)
 *   | Mem (keys, b, ctx) -> Mem (flatten_path keys, b, ctx)
 *   | Mem_tree (keys, b, ctx) -> Mem_tree (flatten_path keys, b, ctx)
 *   | Commit _ as op -> op *)

let open_row_sequence max_block_count path_conversion path : Def.row Seq.t =
  let _convert_path =
    match path_conversion with
    | `None -> Fun.id
    | `V1 -> flatten_v1
    | `V0 -> flatten_v0
    | `V0_and_v1 -> fun p -> flatten_v1 p |> flatten_v0
  in
  (* TODO: Re-enable convert_path *)
  let aux (ops_seq, block_sent_count) =
    if block_sent_count >= max_block_count then None
    else
      match ops_seq () with
      (* _ -> failwith "super" *)
      | Seq.Nil ->
          (* TODO: I should raise here since this will be clipped *)
          None
      | Cons (row, ops_sec) -> Some (row, (ops_sec, block_sent_count + 1))
  in

  let _header, ops_seq = Def.open_reader path in
  Seq.unfold aux (ops_seq, 0)

module Make (Store : Store) = struct
  module Stat_collector = Trace_collection.Make_stat (Store)
  module Context = Tezos_context.Context.Make (Store)

  (* mutable latest_commit : Store.Hash.t option; *)
  (* TODO: Some to assoc *)
  type replay_state = {
    index : Context.index;
    contexts : (int64, Context.t) Hashtbl.t;
    trees : (int64, Context.tree) Hashtbl.t;
    hash_corresps : (Def.hash, Context_hash.t) Hashtbl.t;
    check_hashes : bool;
    empty_blobs : bool;
    mutable current_block_idx : int;
    mutable current_row : Def.row;
    mutable current_event_idx : int;
    mutable recursion_depth : int;
  }

  type cold = [ `Cold of config ]
  type warm = [ `Warm of replay_state ]
  type t = [ cold | warm ]

  let check_hash_trace hash_trace hash_replayed =
    let hash_replayed = Context_hash.to_string hash_replayed in
    if hash_trace <> hash_replayed then
      Fmt.failwith "hash replay %s, hash trace %s" hash_replayed hash_trace

  let get_ok = function
    | Error e -> Fmt.(str "%a" (list Tezos_base.TzPervasives.pp) e) |> failwith
    | Ok h -> h

  (* let error_find op k b n_op n_c in_ctx_id =
   *   Fmt.failwith
   *     "Cannot reproduce operation %d on ctx %Ld of commit %d %s @[k = %a@] \
   *      expected %b"
   *     n_op in_ctx_id n_c op
   *     Fmt.(list ~sep:comma string)
   *     k b *)

  let on_rhs_tree t (scope_end, tracker) tree =
    match scope_end with
    | Def.Last_occurence -> ()
    | Will_reoccur -> Hashtbl.add t.trees tracker tree

  let on_rhs_context t (scope_end, tracker) context =
    match scope_end with
    | Def.Last_occurence -> ()
    | Will_reoccur -> Hashtbl.add t.contexts tracker context

  let on_rhs_hash t (scope_start, scope_end, hash_trace) hash_replayed =
    if t.check_hashes then check_hash_trace hash_trace hash_replayed;
    match (scope_start, scope_end) with
    | Def.First_instanciation, Def.Last_occurence -> ()
    | First_instanciation, Will_reoccur ->
        Hashtbl.add t.hash_corresps hash_trace hash_replayed
    | Reinstanciation, Last_occurence ->
        Hashtbl.remove t.hash_corresps hash_trace
    | Reinstanciation, Will_reoccur ->
        (* This may occur is 2 commits of the replay have the same hash *)
        ()

  let on_lhs_context t (scope_end, tracker) =
    let v =
      Hashtbl.find t.contexts tracker
      (* Shoudn't fail because it should follow a [save_context]. *)
    in
    if scope_end = Def.Last_occurence then Hashtbl.remove t.contexts tracker;
    v

  let on_lhs_hash t (scope_start, scope_end, hash_trace) =
    match (scope_start, scope_end) with
    | Def.Instanciated, Def.Last_occurence ->
        let v = Hashtbl.find t.hash_corresps hash_trace in
        Hashtbl.remove t.hash_corresps hash_trace;
        v
    | Instanciated, Def.Will_reoccur -> Hashtbl.find t.hash_corresps hash_trace
    | Not_instanciated, (Def.Last_occurence | Def.Will_reoccur) ->
        (* This hash has not been seen yet out of a [commit] or [commit_genesis],
           this implies that [hash_trace] exist in the store prior to replay.

           The typical occurence of that situation is the first checkout of a
           replay starting from an existing store. *)
        Context_hash.of_string_exn hash_trace

  let exec_tree_empty t ((), tr) =
    let tr' = Context.Tree.empty 42 in
    on_rhs_tree t tr tr';
    Lwt.return_unit

  let exec_checkout t (hash, c) =
    let hash = on_lhs_hash t hash in
    let* c' = Context.checkout t.index hash in
    let c' = match c' with None -> failwith "Checkout failed" | Some x -> x in
    on_rhs_context t c c';
    Lwt.return_unit

  let exec_commit_genesis t ((chain_id, time, protocol), hash) =
    let chain_id = Tezos_base.TzPervasives.Chain_id.of_string_exn chain_id in
    let time = Tezos_base.Time.Protocol.of_seconds time in
    let protocol =
      Tezos_base.TzPervasives.Protocol_hash.of_string_exn protocol
    in
    let* hash' = Context.commit_genesis ~time ~protocol ~chain_id t.index in
    let hash' = get_ok hash' in
    on_rhs_hash t hash hash';
    Lwt.return_unit

  let exec_commit t ((time, message, c), hash) =
    let time = Tezos_base.Time.Protocol.of_seconds time in
    let c = on_lhs_context t c in
    let* hash' = Context.commit ~time ?message c in
    on_rhs_hash t hash hash';
    Lwt.return_unit

  let rec exec_init (config : config) (row : Def.row) (readonly, ()) =
    let tref = ref None in
    let patch_context c =
      let t = Option.get !tref in
      (match t.current_row.ops.(t.current_event_idx) with
      | Commit_genesis _ -> ()
      | _ -> assert false);
      let* () = exec_patch_context_enter t c in
      match t.current_row.ops.(t.current_event_idx) with
      | Patch_context_exit (c, d) ->
          let _c' : Context.t = on_lhs_context t c in
          let d' = on_lhs_context t d in
          Lwt.return (Ok d')
      | _ -> assert false
    in
    let patch_context =
      if row.uses_patch_context then Some patch_context else None
    in
    (* TODO: Move checks out of main loop *)
    if Sys.file_exists config.store_dir then
      invalid_arg "Can't open irmin-pack store. Destination already exists";
    (match config.startup_store_type with
    | `Fresh -> ()
    | `Copy_from origin ->
        let ( / ) = Filename.concat in
        let exec cmd args =
          let cmd = Filename.quote_command cmd args in
          Printf.eprintf "cmd: %s\n%!" cmd;
          let err = Sys.command cmd in
          if err <> 0 then
            Fmt.failwith
              "Could not prepare startup store. Got error code %d for %s" err
              cmd
        in
        exec "cp" [ "-r"; origin; config.store_dir ];
        exec "tree" [ config.store_dir ];
        (Sys.readdir config.store_dir
        |> Array.to_list
        |> List.map (fun fname -> config.store_dir / fname))
        @ (Sys.readdir (config.store_dir / "index")
          |> Array.to_list
          |> List.map (fun fname -> config.store_dir / "index" / fname))
        |> List.filter (fun p -> not @@ Sys.is_directory p)
        |> List.iter (fun p -> exec "chmod" [ "644"; p ]));

    Printf.eprintf "Let's go for Context.init\n%!";
    let* index = Context.init ?readonly ?patch_context config.store_dir in
    let t =
      {
        index;
        contexts = Hashtbl.create 3;
        trees = Hashtbl.create 3;
        hash_corresps = Hashtbl.create 3;
        check_hashes = should_check_hashes config;
        empty_blobs = config.empty_blobs;
        current_block_idx = 0;
        current_row = row;
        current_event_idx = 0;
        recursion_depth = 0;
      }
    in
    tref := Some t;
    Lwt.return t

  and exec_patch_context_enter t c' =
    t.recursion_depth <- t.recursion_depth + 1;
    t.current_event_idx <- t.current_event_idx + 1;
    let events = t.current_row.Def.ops in
    let i = t.current_event_idx in
    let ev = events.(i) in
    match ev with
    | Def.Patch_context_enter c ->
        on_rhs_context t c c';
        exec_next_event t
    | _ -> assert false

  and exec_next_event t =
    t.current_event_idx <- t.current_event_idx + 1;
    let events = t.current_row.Def.ops in
    let commit_idx = Array.length events - 1 in
    let i = t.current_event_idx in
    let ev = events.(i) in
    if i = commit_idx then (
      assert (t.recursion_depth = 0);
      match ev with
      | Def.Commit data -> exec_commit t data
      | Commit_genesis data -> exec_commit_genesis t data
      | _ -> assert false)
    else (exec_nonlast_event [@tailcall]) t ev

  and exec_nonlast_event t ev =
    (match ev with
    | Def.Tree ev -> (
        match ev with
        | Empty data -> exec_tree_empty t data
        | _ -> failwith "super 1")
    | Init _ | Commit_genesis _ | Commit _ -> assert false
    | Checkout data -> exec_checkout t data
    | _ -> Fmt.failwith "super 2: %a" (Repr.pp Def.event_t) ev)
    >>= fun () -> (exec_next_event [@tailcall]) t

  let exec_block : [< t ] -> _ -> _ -> warm Lwt.t =
   fun t row block_idx ->
    Printf.eprintf "exec block\n%!";
    let exec_very_first_event config =
      assert (block_idx = 0);
      let events = row.Def.ops in
      let ev = events.(0) in
      match ev with
      | Def.Init data -> exec_init config row data
      | _ -> assert false
    in
    match t with
    | `Cold config ->
        let* t = exec_very_first_event config in
        let* () = exec_next_event t in
        Lwt.return (`Warm t)
    | `Warm t ->
        t.current_block_idx <- block_idx;
        t.current_row <- row;
        t.current_event_idx <- -1;
        let* () = exec_next_event t in
        Lwt.return (`Warm t)

  let exec_blocks config row_seq stats =
    with_progress_bar ~message:"Replaying trace" ~n:config.ncommits_trace
      ~unit:"commits"
    @@ fun prog ->
    ignore stats;
    ignore prog;
    Printf.eprintf "\n%!";

    let rec aux t commit_idx row_seq =
      Printf.eprintf "aux on commit idx %d\n%!" commit_idx;
      match row_seq () with
      | Seq.Nil ->
          Printf.eprintf "nil\n%!";
          (* on_end () >|= fun () -> commit_idx *)
          Lwt.return commit_idx
      | Cons (row, row_seq) ->
          Printf.eprintf "cons\n%!";
          let* t = exec_block t row commit_idx in
          let t = (t :> t) in
          (* let len0 = Hashtbl.length t.contexts in
           * let len1 = Hashtbl.length t.hash_corresps in
           * if (len0, len1) <> (0, 1) then
           *   Logs.app (fun l ->
           *       l "\nAfter commit %6d we have %d/%d history sizes" i len0 len1); *)
          (* let* () = on_commit i (Option.get t.latest_commit) in *)
          (* prog Int64.one; *)
          aux t (commit_idx + 1) row_seq
    in
    aux (`Cold config) 0 row_seq

  let run ext_config config =
    let check_hashes = should_check_hashes config in
    ignore ext_config;
    Logs.app (fun l ->
        l "Will %scheck commit hashes against reference."
          (if check_hashes then "" else "NOT "));
    let row_seq =
      open_row_sequence config.ncommits_trace config.path_conversion
        config.commit_data_file
    in
    (* let* repo, on_commit, on_end, repo_pp = Store.create_repo' ext_config in *)
    prepare_artefacts_dir config.artefacts_dir;
    let stat_path = Filename.concat config.artefacts_dir "stat_trace.repr" in
    let c =
      let entries, stable_hash = config.inode_config in
      Trace_definitions.Stat_trace.
        {
          setup =
            `Replay
              {
                path_conversion = config.path_conversion;
                artefacts_dir = config.artefacts_dir;
              };
          inode_config = (entries, entries, stable_hash);
          store_type = config.store_type;
        }
    in
    let stats = Stat_collector.create_file stat_path c config.store_dir in
    let+ summary_opt =
      Lwt.finalize
        (fun () ->
          let+ block_count = exec_blocks config row_seq stats in
          Logs.app (fun l -> l "Closing repo...");
          (* let+ () = Store.Repo.close repo in *)
          Stat_collector.close stats;
          if not config.no_summary then (
            Logs.app (fun l -> l "Computing summary...");
            Some (Trace_stat_summary.summarise ~block_count stat_path))
          else None)
        (fun () ->
          if config.keep_stat_trace then (
            Logs.app (fun l -> l "Stat trace kept at %s" stat_path);
            Unix.chmod stat_path 0o444;
            Lwt.return_unit)
          else (
            Sys.remove stat_path;
            Lwt.return_unit))
    in
    ignore summary_opt;
    fun ppf -> Format.fprintf ppf "\nsuper 3\n"
  (* match summary_opt with
   * | Some summary ->
   *     let p = Filename.concat config.artefacts_dir "boostrap_summary.json" in
   *     Trace_stat_summary.save_to_json summary p;
   *     fun ppf ->
   *       Format.fprintf ppf "\n%t\n%a" repo_pp
   *         (Trace_stat_summary_pp.pp 5)
   *         ([ "" ], [ summary ])
   * | None -> fun ppf -> Format.fprintf ppf "\n%t\n" repo_pp *)
end
