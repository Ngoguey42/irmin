open Bench_common

let ( >>= ) = Lwt.Infix.( >>= )
let ( >|= ) = Lwt.Infix.( >|= )
let ( let* ) x f = Lwt.bind x f
let ( let+ ) x f = Lwt.map f x

type key = string list [@@deriving yojson]
type hash = string [@@deriving yojson]
type message = string [@@deriving yojson]

type op =
  | Add of key * string
  | Remove of key
  | Find of key * bool
  | Mem of key * bool
  | Mem_tree of key * bool
  | Commit of hash * int64 * message * hash list
  | Checkout of hash
  | Copy of key * key
[@@deriving yojson]

module Parse_trace = struct
  let is_hex_char = function
    | '0' .. '9' | 'a' .. 'f' | 'A' .. 'F' -> true
    | _ -> false

  let is_2char_hex s =
    if String.length s <> 2 then false
    else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char

  let is_30char_hex s =
    if String.length s <> 30 then false
    else s |> String.to_seq |> List.of_seq |> List.for_all is_hex_char

  let rec collapse_key_suffix = function
    | a :: b :: c :: d :: e :: f :: tl
      when is_2char_hex a
           && is_2char_hex b
           && is_2char_hex c
           && is_2char_hex d
           && is_2char_hex e
           && is_30char_hex f ->
        (a ^ b ^ c ^ d ^ e ^ f) :: collapse_key_suffix tl
    | hd :: tl -> hd :: collapse_key_suffix tl
    | [] -> []

  (** This function collapses all the 6 step-long chunks forming 38 byte-long
      hashes to a single step.

      The paths in tezos:
      https://www.dailambda.jp/blog/2020-05-11-plebeia/#tezos-path *)
  let collapse_key = collapse_key_suffix

  let collapse_op = function
    | Add (key, v) -> Add (collapse_key key, v)
    | Remove keys -> Remove (collapse_key keys)
    | Find (keys, b) -> Find (collapse_key keys, b)
    | Mem (keys, b) -> Mem (collapse_key keys, b)
    | Mem_tree (keys, b) -> Mem_tree (collapse_key keys, b)
    | Checkout _ as op -> op
    | Copy (from, to_) -> Copy (collapse_key from, collapse_key to_)
    | Commit _ as op -> op

  let read_commits commits ncommits collapse =
    let parse_op op =
      if collapse then op_of_yojson op |> Result.map collapse_op
      else op_of_yojson op
    in
    let json = Dataset.get_stream () in
    let rec aux index_op index_commit operations =
      if index_commit >= ncommits then index_commit
      else
        match Stream.next json with
        | exception Stream.Failure ->
            Fmt.epr
              "Only %d commits available in the trace file, proceeding...\n%!"
              index_commit;
            index_commit
        | op -> (
            match parse_op op with
            | Ok (Commit _ as x) ->
                commits.(index_commit) <- List.rev (x :: operations);
                (aux [@tailcall]) (index_op + 1) (index_commit + 1) []
            | Ok x ->
                (aux [@tailcall]) (index_op + 1) index_commit (x :: operations)
            | Error s -> Fmt.failwith "error op_of_yosjon %s\n%!" s)
    in
    aux 0 0 []

  let populate_array ncommits collapse =
    let commits = Array.init ncommits (fun _ -> []) in
    let n = read_commits commits ncommits collapse in
    (commits, n)
end

module Generate_trees_from_trace
    (Store : Irmin.S with type contents = string and type key = string list) =
struct
  type t = { mutable tree : Store.tree }

  type stat_entry =
    [ `Add | `Remove | `Find | `Mem | `Mem_tree | `Checkout | `Copy | `Commit ]
  [@@deriving repr]

  let op_tags =
    [ `Add; `Remove; `Find; `Mem; `Mem_tree; `Checkout; `Copy; `Commit ]

  let stats =
    op_tags
    |> List.map (fun which -> (which, (0, Mtime.Span.zero)))
    |> List.to_seq
    |> Hashtbl.of_seq

  let pp_stats fmt () =
    let total =
      Hashtbl.to_seq stats
      |> Seq.map snd
      |> Seq.map snd
      |> Seq.fold_left Mtime.Span.add Mtime.Span.zero
      |> Mtime.Span.to_s
    in
    let total = if total = 0. then 1. else total in
    let pp_stat fmt which =
      let n, el = Hashtbl.find stats which in
      let el = Mtime.Span.to_s el in
      Format.fprintf fmt "%d %a %.3f sec (%.1f%%)" n (Repr.pp stat_entry_t)
        which el
        (el /. total *. 100.)
    in
    Fmt.pf fmt "%a" Fmt.(list ~sep:(any "@\n") pp_stat) op_tags

  let with_monitoring which f =
    let n0, el0 = Hashtbl.find stats which in
    let t0 = Mtime_clock.counter () in
    let+ res = f () in
    let el1 = Mtime_clock.count t0 in
    Hashtbl.replace stats which (n0 + 1, Mtime.Span.add el0 el1);
    res

  let error_find op k b n_op n_c =
    Fmt.failwith
      "Cannot reproduce operation %d of commit %d %s @[k = %a@] expected %b"
      n_op n_c op
      Fmt.(list ~sep:comma string)
      k b

  let exec_add t _repo prev_commit _n i key v () =
    let+ tree = Store.Tree.add t.tree key v in
    t.tree <- tree;
    (i + 1, prev_commit)

  let exec_remove t _repo prev_commit _n i keys () =
    let+ tree = Store.Tree.remove t.tree keys in
    t.tree <- tree;
    (i + 1, prev_commit)

  let exec_find t _repo prev_commit n i keys b () =
    Store.Tree.find t.tree keys >|= function
    | None when not b -> (i + 1, prev_commit)
    | Some _ when b -> (i + 1, prev_commit)
    | _ -> error_find "find" keys b i n

  let exec_mem t _repo prev_commit n i keys b () =
    let+ b' = Store.Tree.mem t.tree keys in
    if b <> b' then error_find "mem" keys b i n;
    (i + 1, prev_commit)

  let exec_mem_tree t _repo prev_commit n i keys b () =
    let+ b' = Store.Tree.mem_tree t.tree keys in
    if b <> b' then error_find "mem_tree" keys b i n;
    (i + 1, prev_commit)

  let exec_checkout t repo prev_commit _n i () =
    Option.get prev_commit |> Store.Commit.of_hash repo >|= function
    | None -> Fmt.failwith "prev commit not found"
    | Some commit ->
        let tree = Store.Commit.tree commit in
        t.tree <- tree;
        (i + 1, prev_commit)

  let exec_copy t _repo prev_commit _n i from to_ () =
    Store.Tree.find_tree t.tree from >>= function
    | None -> Lwt.return (i + 1, prev_commit)
    | Some sub_tree ->
        let+ tree = Store.Tree.add_tree t.tree to_ sub_tree in
        t.tree <- tree;
        (i + 1, prev_commit)

  let exec_commit t repo prev_commit _n i date message () =
    (* in tezos commits call Tree.list first for the unshallow operation *)
    let* _ = Store.Tree.list t.tree [] in
    let info = Irmin.Info.v ~date ~author:"Tezos" message in
    let parents = match prev_commit with None -> [] | Some p -> [ p ] in
    let+ commit = Store.Commit.v repo ~info ~parents t.tree in
    Store.Tree.clear t.tree;
    (i + 1, Some (Store.Commit.hash commit))

  let add_operations t repo prev_commit operations n =
    Lwt_list.fold_left_s
      (fun (i, prev_commit) (operation : op) ->
        match operation with
        | Add (key, v) ->
            exec_add t repo prev_commit n i key v |> with_monitoring `Add
        | Remove keys ->
            exec_remove t repo prev_commit n i keys |> with_monitoring `Remove
        | Find (keys, b) ->
            exec_find t repo prev_commit n i keys b |> with_monitoring `Find
        | Mem (keys, b) ->
            exec_mem t repo prev_commit n i keys b |> with_monitoring `Mem
        | Mem_tree (keys, b) ->
            exec_mem_tree t repo prev_commit n i keys b
            |> with_monitoring `Mem_tree
        | Checkout _ ->
            exec_checkout t repo prev_commit n i |> with_monitoring `Checkout
        | Copy (from, to_) ->
            exec_copy t repo prev_commit n i from to_ |> with_monitoring `Copy
        | Commit (_, date, message, _) ->
            exec_commit t repo prev_commit n i date message
            |> with_monitoring `Commit)
      (0, prev_commit) operations

  let add_commits repo commits () =
    let t = { tree = Store.Tree.empty } in
    let rec array_iter_lwt prev_commit i =
      if i >= Array.length commits then Lwt.return_unit
      else
        let operations = commits.(i) in
        let* _, prev_commit = add_operations t repo prev_commit operations i in
        array_iter_lwt prev_commit (i + 1)
    in
    array_iter_lwt None 0
end

type config = {
  ncommits : int;
  ncommits_trace : int;
  depth : int;
  nchain_trees : int;
  width : int;
  nlarge_trees : int;
  operations_file : string;
  root : string;
  suite_filter :
    [ `Slow
    | `Quick
    | `Quick_trace
    | `Quick_large
    | `Quick_chains
    | `Slow_trace
    | `Slow_large
    | `Slow_chains ];
  collapse : bool;
}

module Benchmark = struct
  type result = { time : float; size : int }

  let run config f =
    let+ time, _ = with_timer f in
    let size = FSHelper.get_size config.root in
    { time; size }

  let pp_results fmt result =
    Format.fprintf fmt "Total time: %f@\nSize on disk: %d M" result.time
      result.size
end

module Hash = Irmin.Hash.SHA1

module Bench_suite (Conf : sig
  val entries : int
  val stable_hash : int
end) =
struct
  module Store =
    Irmin_pack.Make (Conf) (Irmin.Metadata.None) (Irmin.Contents.String)
      (Irmin.Path.String_list)
      (Irmin.Branch.String)
      (Hash)

  let init_commit repo =
    Store.Commit.v repo ~info:(info ()) ~parents:[] Store.Tree.empty

  module Trees = Generate_trees (Store)
  module Trees_trace = Generate_trees_from_trace (Store)

  let checkout_and_commit repo prev_commit f =
    Store.Commit.of_hash repo prev_commit >>= function
    | None -> Lwt.fail_with "commit not found"
    | Some commit ->
        let tree = Store.Commit.tree commit in
        let* tree = f tree in
        Store.Commit.v repo ~info:(info ()) ~parents:[ prev_commit ] tree

  let add_commits repo ncommits f () =
    let* c = init_commit repo in
    let rec aux c i =
      if i >= ncommits then Lwt.return c
      else
        let* c' = checkout_and_commit repo (Store.Commit.hash c) f in
        aux c' (i + 1)
    in
    let+ _ = aux c 0 in
    ()

  let run ~mode config =
    reset_stats ();
    let conf = Irmin_pack.config ~readonly:false ~fresh:true config.root in
    let* repo = Store.Repo.v conf in
    let* result =
      (match mode with
      | `Large ->
          Trees.add_large_trees config.width config.nlarge_trees
          |> add_commits repo config.ncommits
      | `Chains ->
          Trees.add_chain_trees config.depth config.nchain_trees
          |> add_commits repo config.ncommits)
      |> Benchmark.run config
    in
    let+ () = Store.Repo.close repo in
    (config, result)

  let run_read_trace ?quick config =
    reset_stats ();
    let ncommits = if quick = Some () then 10000 else config.ncommits_trace in
    let commits, n = Parse_trace.populate_array ncommits config.collapse in
    let config = { config with ncommits_trace = n } in
    let conf = Irmin_pack.config ~readonly:false ~fresh:true config.root in
    let* repo = Store.Repo.v conf in
    let* result =
      Trees_trace.add_commits repo commits |> Benchmark.run config
    in
    let+ () = Store.Repo.close repo in
    (config, result)
end

module Bench_inodes_32 = Bench_suite (Conf)

module Bench_inodes_2 = Bench_suite (struct
  let entries = 2
  let stable_hash = 5
end)

type mode_elt = [ `Read_trace | `Chains | `Large ]

type suite_elt = {
  mode : mode_elt;
  speed : [ `Quick | `Slow ];
  inode_config : [ `Entries_32 | `Entries_2 ];
  run : config -> (config * Benchmark.result) Lwt.t;
}

let suite : suite_elt list =
  [
    {
      mode = `Read_trace;
      speed = `Quick;
      inode_config = `Entries_32;
      run = Bench_inodes_32.run_read_trace ~quick:();
    };
    {
      mode = `Read_trace;
      speed = `Slow;
      inode_config = `Entries_32;
      run = Bench_inodes_32.run_read_trace;
    };
    {
      mode = `Chains;
      speed = `Quick;
      inode_config = `Entries_32;
      run = Bench_inodes_32.run ~mode:`Chains;
    };
    {
      mode = `Chains;
      speed = `Slow;
      inode_config = `Entries_2;
      run = Bench_inodes_2.run ~mode:`Chains;
    };
    {
      mode = `Large;
      speed = `Quick;
      inode_config = `Entries_32;
      run = Bench_inodes_32.run ~mode:`Large;
    };
    {
      mode = `Large;
      speed = `Slow;
      inode_config = `Entries_2;
      run = Bench_inodes_2.run ~mode:`Large;
    };
  ]

let pp_inode_config fmt = function
  | `Entries_2 -> Format.fprintf fmt "[2, 5]"
  | `Entries_32 -> Format.fprintf fmt "[32, 256]"

let pp_config b config fmt () =
  match b.mode with
  | `Read_trace ->
      Format.fprintf fmt "Tezos_log mode on inode config %a: %d commits @\n%a"
        pp_inode_config b.inode_config config.ncommits_trace
        Bench_inodes_32.Trees_trace.pp_stats ()
  | `Chains ->
      Format.fprintf fmt
        "Chain trees mode on inode config %a: %d commits, each consisting of \
         %d chains of depth %d"
        pp_inode_config b.inode_config config.ncommits config.nchain_trees
        config.depth
  | `Large ->
      Format.fprintf fmt
        "Large trees mode on inode config %a: %d commits, each consisting of \
         %d large trees of %d entries"
        pp_inode_config b.inode_config config.ncommits config.nlarge_trees
        config.width

let main ncommits ncommits_trace operations_file suite_filter collapse depth
    width nchain_trees nlarge_trees =
  let config =
    {
      ncommits;
      ncommits_trace;
      operations_file;
      root = "test-bench";
      suite_filter;
      collapse;
      depth;
      width;
      nchain_trees;
      nlarge_trees;
    }
  in
  Printexc.record_backtrace true;
  Random.self_init ();
  FSHelper.rm_dir config.root;
  let suite =
    List.filter
      (fun { mode; speed; _ } ->
        match (suite_filter, speed, mode) with
        | `Slow, `Quick, `Read_trace ->
            (* The suite contains two `Read_trace benchmarks, let's keep the
               `Slow one only *)
            false
        | `Slow, _, _ -> true
        | `Quick, `Quick, _ -> true
        | `Quick_trace, `Quick, `Read_trace -> true
        | `Quick_chains, `Quick, `Chains -> true
        | `Quick_large, `Quick, `Large -> true
        | `Slow_trace, `Slow, `Read_trace -> true
        | `Slow_chains, `Slow, `Chains -> true
        | `Slow_large, `Slow, `Large -> true
        | _, _, _ -> false)
      suite
  in

  let run_benchmarks () =
    Lwt_list.fold_left_s
      (fun (config, results) (b : suite_elt) ->
        let+ config, result = b.run config in
        (config, (b, result) :: results))
      (config, []) suite
  in
  let config, results = Lwt_main.run (run_benchmarks ()) in
  let pp_result fmt (b, result) =
    Format.fprintf fmt "Configuration:@\n @[%a@]@\n@\nResults:@\n @[%a@]@\n"
      (pp_config b config) () Benchmark.pp_results result
  in
  Fmt.pr "%a@." Fmt.(list ~sep:(any "@\n@\n") pp_result) results

open Cmdliner

let suite =
  let suite =
    [
      ("slow", `Slow);
      ("quick", `Quick);
      ("quick_trace", `Quick_trace);
      ("quick_chains", `Quick_chains);
      ("quick_large", `Quick_large);
      ("slow_trace", `Slow_trace);
      ("slow_chains", `Slow_chains);
      ("slow_large", `Slow_large);
    ]
  in
  let doc = Arg.info ~doc:(Arg.doc_alts_enum suite) [ "suite" ] in
  Arg.(value @@ opt (Arg.enum suite) `Slow doc)

let collapse =
  let doc =
    Arg.info ~doc:"Collapse the paths in the trace benchmarks" [ "collapse" ]
  in
  Arg.(value @@ flag doc)

let ncommits =
  let doc =
    Arg.info ~doc:"Number of commits for the large and chain modes."
      [ "n"; "ncommits" ]
  in
  Arg.(value @@ opt int 2 doc)

let ncommits_trace =
  let doc =
    Arg.info ~doc:"Number of commits to read from trace." [ "ncommits_trace" ]
  in
  Arg.(value @@ opt int 13315 doc)

let depth =
  let doc =
    Arg.info ~doc:"Depth of a commit's tree in chains-mode." [ "d"; "depth" ]
  in
  Arg.(value @@ opt int 1000 doc)

let nchain_trees =
  let doc =
    Arg.info ~doc:"Number of chain trees per commit in chains-mode."
      [ "c"; "nchain" ]
  in
  Arg.(value @@ opt int 1 doc)

let width =
  let doc =
    Arg.info ~doc:"Width of a commit's tree in large-mode." [ "w"; "width" ]
  in
  Arg.(value @@ opt int 1000000 doc)

let nlarge_trees =
  let doc =
    Arg.info ~doc:"Number of large trees per commit in large-mode."
      [ "l"; "nlarge" ]
  in
  Arg.(value @@ opt int 1 doc)

let operations_file =
  let doc =
    Arg.info ~doc:"Compressed file containing the tree operations."
      [ "f"; "file" ]
  in
  Arg.(value @@ opt string "bench/irmin-pack/tezos_log.tar.gz" doc)

let main_term =
  Term.(
    const main
    $ ncommits
    $ ncommits_trace
    $ operations_file
    $ suite
    $ collapse
    $ depth
    $ width
    $ nchain_trees
    $ nlarge_trees)

let () =
  let info = Term.info "Benchmarks for tree operations" in
  Term.exit @@ Term.eval (main_term, info)
