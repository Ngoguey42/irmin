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

module Summary = Trace_stat_summary
module Utils = Trace_stat_summary_utils

type metrics = (string * float list) list [@@deriving repr]

let metrics_t = Irmin.Type.(Json.assoc (list float))

type result_ = { name : string; metrics : metrics } [@@deriving repr]
type t = { results : result_ list } [@@deriving repr]

(** linear_bag_stat, point-to-point.
    A [Summary.linear_bag_stat] is a datastructure representing a variable.
    Point to point is the lasted value encountered minus the first. *)
let lbs_ptp block_count vs =
  let open Summary in
  ignore block_count;
  (* vs.diff_per_block.mean *. float_of_int block_count |> Float.round *)
  fst vs.value_after_commit.max_value -. fst vs.value_after_commit.min_value

let filter_nans_out results =
  List.map
    (fun { name; metrics } ->
      let metrics =
        List.map
          (fun (name, l) ->
            (name, match l with [ v ] when Float.is_nan v -> [] | l -> l))
          metrics
      in
      { name; metrics })
    results

let create_raw_results0 s : result_ list =
  let open Summary in
  [
    {
      name = "Trace Replay - main metrics";
      metrics =
        [
          ("CPU time elapsed (s)", [ s.elapsed_cpu ]);
          ( "TZ-transactions per sec",
            [
              (Utils.approx_transaction_count_of_block_count s.block_count
              |> float_of_int)
              /. s.elapsed_cpu;
            ] );
          ( "TZ-operations per sec",
            [
              (Utils.approx_operation_count_of_block_count s.block_count
              |> float_of_int)
              /. s.elapsed_cpu;
            ] );
          ( "Context.set per sec",
            [
              fst (Span.Map.find `Add s.span).cumu_count.max_value
              /. s.elapsed_cpu;
            ] );
          ( "tail latency (s)",
            [ fst (Span.Map.find `Commit s.span).duration.max_value ] );
        ];
    };
    {
      name = "Trace Replay - resource usage - disk IO (total)";
      metrics =
        [
          ( "IOPS (op/s)",
            [ lbs_ptp s.block_count s.index.nb_both /. s.elapsed_cpu ] );
          ( "throughput (MB/s)",
            [ lbs_ptp s.block_count s.index.bytes_both /. s.elapsed_cpu /. 1e6 ]
          );
          ("total (MB)", [ lbs_ptp s.block_count s.index.bytes_both /. 1e6 ]);
        ];
    };
    {
      name = "Trace Replay - resource usage - disk IO (read)";
      metrics =
        [
          ( "IOPS (op/s)",
            [ lbs_ptp s.block_count s.index.nb_reads /. s.elapsed_cpu ] );
          ( "throughput (MB/s)",
            [ lbs_ptp s.block_count s.index.bytes_read /. s.elapsed_cpu /. 1e6 ]
          );
          ("total (MB)", [ lbs_ptp s.block_count s.index.bytes_read /. 1e6 ]);
        ];
    };
    {
      name = "Trace Replay - resource usage - disk IO (write)";
      metrics =
        [
          ( "IOPS (op/s)",
            [ lbs_ptp s.block_count s.index.nb_writes /. s.elapsed_cpu ] );
          ( "throughput (MB/s)",
            [
              lbs_ptp s.block_count s.index.bytes_written
              /. s.elapsed_cpu
              /. 1e6;
            ] );
          ("total (MB)", [ lbs_ptp s.block_count s.index.bytes_written /. 1e6 ]);
        ];
    };
    {
      name = "Trace Replay - resource usage - misc.";
      metrics =
        [
          ( "max memory usage (GB)",
            [ List.fold_left max 0. s.gc.major_heap_top_bytes /. 1e9 ] );
          ("mean CPU usage", [ s.elapsed_cpu /. s.elapsed_wall ]);
        ];
    };
  ]

let create_raw_results1 s : result_ list =
  let a =
    let name = "Context Phases Length" in
    let open Trace_stat_summary in
    let metrics =
      [ `Block; `Buildup; `Commit ]
      |> List.map @@ fun k ->
         let vs = Span.(Map.find k s.span).duration in
         ( Printf.sprintf "%s (ms)" (Span.Key.to_string k),
           [
             vs.mean *. 1000.;
             (* TODO: {min / max / avg}, maybe on a log scale? *)
           ] )
    in
    { name; metrics }
  in
  let b =
    let name = "Context Buildup Lengths" in
    let open Trace_stat_summary in
    let metrics =
      [ `Add; `Remove; `Find; `Mem; `Mem_tree; `Copy; `Unseen ]
      |> List.map @@ fun k ->
         let vs = Span.(Map.find k s.span).duration in
         ( Printf.sprintf "%s (\xc2\xb5s)" (Span.Key.to_string k),
           [
             vs.mean *. 1e6; (* TODO: {min / max / avg}, maybe on a log scale? *)
           ] )
    in
    { name; metrics }
  in
  [ a; b ]

let create_raw_results2 s : result_ list =
  let name = "irmin-pack Global Stats" in
  let metrics =
    let open Summary in
    [
      ("finds", s.pack.finds);
      ("cache_misses", s.pack.cache_misses);
      ("appended_hashes", s.pack.appended_hashes);
      ("appended_offsets", s.pack.appended_offsets);
    ]
    |> List.map @@ fun (name, lbs) -> (name, [ lbs_ptp s.block_count lbs ])
  in
  [ { name; metrics } ]

let create_raw_results3 s : result_ list =
  let name = "irmin.tree Global Stats" in
  let open Trace_stat_summary in
  let metrics =
    [
      ("contents_hash", s.tree.contents_hash);
      ("contents_find", s.tree.contents_find);
      ("contents_add", s.tree.contents_add);
      ("node_hash", s.tree.node_hash);
      ("node_mem", s.tree.node_mem);
      ("node_add", s.tree.node_add);
      ("node_find", s.tree.node_find);
      ("node_val_v", s.tree.node_val_v);
      ("node_val_find", s.tree.node_val_find);
      ("node_val_list", s.tree.node_val_list);
    ]
    |> List.map @@ fun (name, lbs) -> (name, [ lbs_ptp s.block_count lbs ])
  in
  [ { name; metrics } ]

let create_raw_results4 s : result_ list =
  let name = "index Stats" in
  let open Trace_stat_summary in
  let metrics =
    [
      ( "cumu data MB",
        [ fst s.index.cumu_data_bytes.value_after_commit.max_value *. 1e-6 ] );
      ("merge count", [ lbs_ptp s.block_count s.index.nb_merge ]);
    ]
  in
  [ { name; metrics } ]

let create_raw_results5 s : result_ list =
  let name = "GC Stats" in
  let open Trace_stat_summary in
  let metrics =
    [
      ("minor words allocated", s.gc.minor_words);
      ("major words allocated", s.gc.major_words);
      ("minor collections", s.gc.minor_collections);
      ("major collections", s.gc.major_collections);
      ("promoted words", s.gc.promoted_words);
      ("compactions", s.gc.compactions);
    ]
    |> List.map @@ fun (name, lbs) -> (name, [ lbs_ptp s.block_count lbs ])
  in
  let metrics' =
    [
      ( "avg major heap MB after commit",
        [ s.gc.major_heap_bytes.value_after_commit.mean /. 1e6 ] );
    ]
  in
  [ { name; metrics = metrics @ metrics' } ]

let of_summary s =
  {
    results =
      create_raw_results0 s
      @ create_raw_results1 s
      @ create_raw_results2 s
      @ create_raw_results3 s
      @ create_raw_results4 s
      @ create_raw_results5 s
      |> filter_nans_out;
  }
