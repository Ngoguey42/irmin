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

module Def = Trace_definitions.Raw_actions_trace
module Utils = Trace_stat_summary_utils

module Op = struct
  module Key = struct
    type t =
      [ `Tree_empty
      | `Tree_of_raw
      | `Tree_of_value
      | `Tree_mem
      | `Tree_mem_tree
      | `Tree_find
      | `Tree_is_empty
      | `Tree_kind
      | `Tree_hash
      | `Tree_equal
      | `Tree_to_value
      | `Tree_clear
      | `Tree_find_tree
      | `Tree_list
      | `Tree_add
      | `Tree_add_tree
      | `Tree_remove
      | `Tree_fold_start
      | `Tree_fold_step_enter
      | `Tree_fold_step_exit
      | `Tree_fold_end
      | `Tree_shallow
      | `Tree_to_raw
      | `Tree_pp
      | `Find_tree
      | `List
      | `Fold_start
      | `Fold_step_enter
      | `Fold_step_exit
      | `Fold_end
      | `Add_tree
      | `Mem
      | `Mem_tree
      | `Find
      | `Get_protocol
      | `Hash
      | `Merkle_tree
      | `Find_predecessor_block_metadata_hash
      | `Find_predecessor_ops_metadata_hash
      | `Get_test_chain
      | `Exists
      | `Retrieve_commit_info
      | `Add
      | `Remove
      | `Add_protocol
      | `Add_predecessor_block_metadata_hash
      | `Add_predecessor_ops_metadata_hash
      | `Add_test_chain
      | `Remove_test_chain
      | `Fork_test_chain
      | `Checkout
      | `Checkout_exn
      | `Close
      | `Sync
      | `Set_master
      | `Set_head
      | `Commit_genesis
      | `Clear_test_chain
      | `Commit
      | `Commit_test_chain_genesis
      | `Init
      | `Patch_context_enter
      | `Patch_context_exit
      | `Restore_context
      | `Restore_integrity
      | `Dump_context
      | `Check_protocol_commit_consistency
      | `Validate_context_hash_consistency_and_commit ]
    [@@deriving repr, enum]

    let all : t list = List.init (max + 1) (fun i -> of_enum i |> Option.get)

    let to_string : [< t ] -> string =
     fun v ->
      match String.split_on_char '"' (Irmin.Type.to_string t v) with
      | [ ""; s; "" ] -> s |> String.lowercase_ascii
      | _ -> failwith "Could not encode span name to json"

    let of_string : string -> (t, [ `Msg of string ]) result =
     fun s ->
      let s = "\"" ^ String.capitalize_ascii s ^ "\"" in
      match Irmin.Type.of_string t s with Ok v -> Ok v | Error _ as e -> e

    let of_row row =
      let open Def in
      match row with
      | Tree (Empty _) -> `Tree_empty
      | Tree (Of_raw _) -> `Tree_of_raw
      | Tree (Of_value _) -> `Tree_of_value
      | Tree (Mem _) -> `Tree_mem
      | Tree (Mem_tree _) -> `Tree_mem_tree
      | Tree (Find _) -> `Tree_find
      | Tree (Is_empty _) -> `Tree_is_empty
      | Tree (Kind _) -> `Tree_kind
      | Tree (Hash _) -> `Tree_hash
      | Tree (Equal _) -> `Tree_equal
      | Tree (To_value _) -> `Tree_to_value
      | Tree (Clear _) -> `Tree_clear
      | Tree (Find_tree _) -> `Tree_find_tree
      | Tree (List _) -> `Tree_list
      | Tree (Add _) -> `Tree_add
      | Tree (Add_tree _) -> `Tree_add_tree
      | Tree (Remove _) -> `Tree_remove
      | Tree (Fold_start _) -> `Tree_fold_start
      | Tree (Fold_step_enter _) -> `Tree_fold_step_enter
      | Tree (Fold_step_exit _) -> `Tree_fold_step_exit
      | Tree (Fold_end _) -> `Tree_fold_end
      | Tree Shallow -> `Tree_shallow
      | Tree To_raw -> `Tree_to_raw
      | Tree Pp -> `Tree_pp
      | Find_tree _ -> `Find_tree
      | List _ -> `List
      | Fold_start _ -> `Fold_start
      | Fold_step_enter _ -> `Fold_step_enter
      | Fold_step_exit _ -> `Fold_step_exit
      | Fold_end _ -> `Fold_end
      | Add_tree _ -> `Add_tree
      | Mem _ -> `Mem
      | Mem_tree _ -> `Mem_tree
      | Find _ -> `Find
      | Get_protocol _ -> `Get_protocol
      | Hash _ -> `Hash
      | Merkle_tree _ -> `Merkle_tree
      | Find_predecessor_block_metadata_hash _ ->
          `Find_predecessor_block_metadata_hash
      | Find_predecessor_ops_metadata_hash _ ->
          `Find_predecessor_ops_metadata_hash
      | Get_test_chain _ -> `Get_test_chain
      | Exists _ -> `Exists
      | Retrieve_commit_info _ -> `Retrieve_commit_info
      | Add _ -> `Add
      | Remove _ -> `Remove
      | Add_protocol _ -> `Add_protocol
      | Add_predecessor_block_metadata_hash _ ->
          `Add_predecessor_block_metadata_hash
      | Add_predecessor_ops_metadata_hash _ ->
          `Add_predecessor_ops_metadata_hash
      | Add_test_chain _ -> `Add_test_chain
      | Remove_test_chain _ -> `Remove_test_chain
      | Fork_test_chain _ -> `Fork_test_chain
      | Checkout _ -> `Checkout
      | Checkout_exn _ -> `Checkout_exn
      | Close -> `Close
      | Sync _ -> `Sync
      | Set_master _ -> `Set_master
      | Set_head _ -> `Set_head
      | Commit_genesis _ -> `Commit_genesis
      | Clear_test_chain _ -> `Clear_test_chain
      | Commit _ -> `Commit
      | Commit_test_chain_genesis _ -> `Commit_test_chain_genesis
      | Init _ -> `Init
      | Patch_context_enter _ -> `Patch_context_enter
      | Patch_context_exit _ -> `Patch_context_exit
      | Restore_context -> `Restore_context
      | Restore_integrity -> `Restore_integrity
      | Dump_context -> `Dump_context
      | Check_protocol_commit_consistency -> `Check_protocol_commit_consistency
      | Validate_context_hash_consistency_and_commit ->
          `Validate_context_hash_consistency_and_commit
  end

  module Make_map (Val : sig
    type t [@@deriving repr]
  end) =
  struct
    include Map.Make (struct
      type t = Key.t

      let compare = compare
    end)

    type map = Val.t t

    let map_t : map Irmin.Type.t =
      let encode map =
        bindings map |> List.map (fun (k, v) -> (Key.to_string k, v))
      in
      let decode l =
        let key_of_string k =
          match Key.of_string k with
          | Ok k -> k
          | Error (`Msg msg) ->
              Fmt.failwith "Could not convert string back to key: %s" msg
        in
        List.map (fun (k, v) -> (key_of_string k, v)) l |> List.to_seq |> of_seq
      in
      Irmin.Type.(map (Json.assoc Val.t) decode encode)
  end
end

module Op_int_map = Op.Make_map (struct
  type t = int [@@deriving repr]
end)

type commit_info = { lvl : int; ops : int } [@@deriving repr]

type segment_info = {
  timestamp_before : float;
  timestamp_after : float;
  concluding_op :
    [ `Commit of commit_info option
    | `Sync
    | `Commit_genesis
    | `Checkout
    | `Unconcluded ];
  ops : Op_int_map.map;
}
[@@deriving repr]

type file_info = {
  op_count : int64;
  pid : int;
  info_per_segment : segment_info list;
}
[@@deriving repr]

type t = { rws : file_info list; ros : file_info list; miscs : file_info list }
[@@deriving repr]

let misc_folder =
  let acc0 = 0L in
  let accumulate acc _ = Int64.succ acc in
  let finalise = Fun.id in
  Utils.Parallel_folders.folder acc0 accumulate finalise

let ops_per_segment_folder =
  let acc0 = ([], (Float.nan, Op_int_map.empty)) in
  let is_ro = ref false in
  let accumulate (older_segments_rev, (timestamp_before, ops)) row =
    let key = Op.Key.of_row row in
    let ops =
      Op_int_map.update key
        (function None -> Some 1 | Some i -> Some (i + 1))
        ops
    in
    (match row with
    | Def.Init (Some true, ()) -> is_ro := true
    | Init _ -> is_ro := false
    | _ -> ());
    let is_concluding =
      match row with
      | Def.Commit (((_, None, _), _), { after; _ }) ->
          `Yes (`Commit None, after)
      | Commit (((_, Some message, _), _), { after; _ }) ->
          let lvl, ops =
            match
              String.split_on_char ',' message
              |> List.map String.trim
              |> List.map (String.split_on_char ' ')
            with
            | [ [ "lvl"; lvl ]; [ "fit"; _ ]; [ "prio"; _ ]; [ ops; "ops" ] ] ->
                (int_of_string lvl, int_of_string ops)
            | _ -> Fmt.failwith "Could not parse commit message: `%s`" message
          in
          `Yes (`Commit (Some { lvl; ops }), after)
      | Commit_genesis (_, { after; _ }) -> `Yes (`Commit_genesis, after)
      | Checkout (_, { after; _ }) when !is_ro -> `Yes (`Checkout, after)
      | Checkout_exn (_, { after; _ }) when !is_ro -> `Yes (`Checkout, after)
      | Sync { after; _ } -> `Yes (`Sync, after)
      | _ -> `No
    in
    match is_concluding with
    | `No -> (older_segments_rev, (timestamp_before, ops))
    | `Yes (concluding_op, t) ->
        let older_segments_rev =
          { timestamp_before; timestamp_after = t; concluding_op; ops }
          :: older_segments_rev
        in
        (older_segments_rev, (t, Op_int_map.empty))
  in
  let finalise (l, (timestamp_before, ops)) =
    let l =
      if Op_int_map.is_empty ops then l
      else
        {
          timestamp_before;
          timestamp_after = Float.nan;
          concluding_op = `Unconcluded;
          ops;
        }
        :: l
    in
    List.rev l
  in
  Utils.Parallel_folders.folder acc0 accumulate finalise

let summarise' pid (row_seq : Def.row Seq.t) =
  let construct op_count info_per_segment =
    { op_count; pid; info_per_segment }
  in
  let pf0 =
    let open Utils.Parallel_folders in
    open_ construct |+ misc_folder |+ ops_per_segment_folder |> seal
  in
  Seq.fold_left Utils.Parallel_folders.accumulate pf0 row_seq
  |> Utils.Parallel_folders.finalise

let parse_trace pid p =
  Printf.eprintf "Parsing trace: %s\n%!" p;
  let (), row_seq = Def.open_reader p in
  summarise' pid row_seq

let parse_directory prefix =
  let traces =
    Def.trace_files_of_trace_directory prefix
    |> List.map (fun (p, pid) -> (p, parse_trace pid p))
  in
  let ros =
    List.filter (fun (p, _) -> Def.type_of_file p = `Ro) traces |> List.map snd
  in
  let rws =
    List.filter (fun (p, _) -> Def.type_of_file p = `Rw) traces |> List.map snd
  in
  let miscs =
    List.filter (fun (p, _) -> Def.type_of_file p = `Misc) traces
    |> List.map snd
  in
  { ros; rws; miscs }

let summarise directory_path = parse_directory directory_path
