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

(** [actions.exe] *)

open Irmin_traces
module Rawdef = Trace_definitions.Raw_actions_trace
module Summary = Trace_raw_actions_summary

let is_trace_magic s = Trace_common.Magic.to_string Rawdef.magic = s

let summarise path =
  Summary.(summarise path |> Fmt.pr "%a\n" (Irmin.Type.pp_json t))

let to_replayable path =
  Lwt_main.run (Trace_raw_actions_to_replayable.run path stdout);
  flush stdout

let list path =
  Rawdef.trace_files_of_trace_directory path
  |> List.iter (fun (path, _) ->
         Fmt.pr "Reading %s\n" path;
         let (), row_seq = Rawdef.open_reader path in
         Seq.iter (Fmt.pr "%a\n" (Repr.pp Rawdef.row_t)) row_seq);
  Fmt.pr "%!"

open Cmdliner

let term_summarise =
  let stat_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const summarise $ stat_trace_file)

let term_to_rep =
  let stat_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const to_replayable $ stat_trace_file)

let term_list =
  let stat_trace_file =
    let doc = Arg.info ~docv:"PATH" ~doc:"A raw actions trace directory" [] in
    Arg.(required @@ pos 0 (some string) None doc)
  in
  Term.(const list $ stat_trace_file)

let () =
  let man = [] in
  let i = Term.info ~man ~doc:"Processing of actions traces." "actions" in

  let man =
    [
      `P "From raw actions trace (directory) to summary (json).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe summarise ./raw_actions/ \
         >./raw_actions_summary.json";
    ]
  in
  let j = Term.info ~man ~doc:"Raw Actions Summary" "summarise" in

  let man =
    [
      `P
        "From raw actions trace (directory) to replayable actions trace (file).";
      `S "EXAMPLE";
      `P
        "manage_actions.exe to-replayable ./raw_actions/ \
         >./replayable_actions.trace ";
    ]
  in
  let k = Term.info ~man ~doc:"Replayable Actions Trace" "to-replayable" in

  let man =
    [ `P "List the operations from a raw actions trace (directory)." ]
  in
  let l = Term.info ~man ~doc:"List Raw Actions" "list" in

  Term.exit
  @@ Term.eval_choice (term_summarise, i)
       [ (term_summarise, j); (term_to_rep, k); (term_list, l) ]
