(*
 * Copyright (c) 2013 Thomas Gazagnaire <thomas@gazagnaire.org>
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

open Lwt
open Cmdliner

let global_option_section = "COMMON OPTIONS"

let pr_str = Format.pp_print_string

let uri_conv =
  let parse str = `Ok (Uri.of_string str) in
  let print ppf v = pr_str ppf (Uri.to_string v) in
  parse, print

let value_conv =
  let parse str = `Ok (IrminValue.Simple.of_bytes str) in
  let print ppf v = pr_str ppf (IrminValue.Simple.to_string v) in
  parse, print

let tag_conv =
  let parse str = `Ok (IrminTag.Simple.of_string str) in
  let print ppf tag = pr_str ppf (IrminTag.Simple.to_string tag) in
  parse, print

let path_conv =
  let parse str = `Ok (IrminTree.Path.of_pretty str) in
  let print ppf path = pr_str ppf (IrminTree.Path.pretty path) in
  parse, print

let value =
  let doc =
    Arg.info ~docv:"VALUE" ~doc:"Value to add." [] in
  Arg.(required & pos ~rev:true 0 (some & value_conv) None & doc)

let path =
  let doc =
    Arg.info ~docv:"PATH" ~doc:"Path." [] in
  Arg.(value & pos 0 path_conv [] & doc)

let default_dir = ".irmin"

(* XXX: ugly hack *)
let init_hook =
  ref (fun () -> ())

let local_store dir =
  IrminLog.msg "source: dir=%s" dir;
  init_hook := (fun () -> if not (Sys.file_exists dir) then Unix.mkdir dir 0o755);
  IrminFS.simple dir

let remote_store uri =
  IrminLog.msg "source: uri=%s" (Uri.to_string uri);
  IrminCRUD.simple uri

let store =
  let in_memory =
    let doc =
      Arg.info ~doc:"In-memory persistence."
        ["m";"in-memory"] in
    Arg.(value & flag & doc) in
  let fs =
    let doc =
      Arg.info ~doc:"File-system persistence." ["f";"file"] in
    Arg.(value & flag & doc) in
  let crud =
    let doc =
      Arg.info ~doc:"CRUD interface."  ["c";"crud"] in
    Arg.(value & flag & doc) in
  let uri =
    let doc = Arg.info ~doc:"Irminsule store location." [] in
    Arg.(value & pos 0 (some string) None & doc) in
  let create in_memory fs crud uri = match in_memory, fs, crud with
    | true , false, false ->
      if uri <> None then IrminLog.error "Non-empty URI, skipping it";
      IrminLog.msg "source: in-memory";
      (module IrminMemory.Simple: Irmin.SIMPLE)
    | false, false, true ->
      let uri = match uri with
        | None   -> Uri.of_string "http://127.0.0.1:8080"
        | Some u -> Uri.of_string u in
      remote_store uri
    | false, true , false ->
      let dir = match uri with
        | None   -> Filename.concat (Sys.getcwd ()) default_dir
        | Some d -> Filename.concat d default_dir in
      local_store dir
    | false, false, false ->
      (* try to guess the correct type *)
      begin match uri with
        | None   -> local_store default_dir
        | Some s ->
          if Sys.file_exists s then local_store s
          else remote_store (Uri.of_string s)
      end
    | _ -> failwith
             (Printf.sprintf "Invalid store source [%b %b %b %s]"
                in_memory fs crud (match uri with None -> "<none>" | Some s -> s))
  in
  Term.(pure create $ in_memory $ fs $ crud $ uri)

let run t =
  Lwt_unix.run (
    catch
      (fun () -> t)
      (function e -> Printf.eprintf "%s\n%!" (Printexc.to_string e); exit 1)
  )

(* INIT *)
let init_doc = "Initialize a store."
let init =
  let doc = init_doc in
  let man = [
    `S "DESCRIPTION";
    `P init_doc;
  ] in
  let daemon =
    let doc =
      Arg.info ~docv:"PORT" ~doc:"Start an Irminsule server on the specified port."
        ["d";"daemon"] in
    Arg.(value & opt (some uri_conv) (Some (Uri.of_string "http://127.0.0.1:8080")) & doc) in
  let init (module S: Irmin.SIMPLE) daemon =
    run begin
      S.create () >>= fun t ->
      !init_hook ();
      match daemon with
      | None     -> return_unit
      | Some uri ->
        IrminLog.msg "daemon: %s" (Uri.to_string uri);
        IrminHTTP.start_server (module S) t uri
    end
  in
  Term.(pure init $ store $ daemon),
  Term.info "init" ~doc ~man

let read_doc = "Read the contents of a node."
let read =
  let doc = read_doc in
  let man = [
    `S "DESCRIPTION";
    `P read_doc;
  ] in
  let read (module S: Irmin.SIMPLE) path =
    run begin
      S.create ()   >>= fun t ->
      S.read t path >>= function
      | None   -> IrminLog.msg "<none>"; exit 1
      | Some v -> IrminLog.msg "%s" (S.Value.pretty v); return_unit
    end
  in
  Term.(pure read $ store $ path),
  Term.info "read" ~doc ~man

let ls_doc = "List subdirectories."
let ls =
  let doc = ls_doc in
  let man = [
    `S "DESCRIPTION";
    `P ls_doc;
  ] in
  let ls (module S: Irmin.SIMPLE) path =
    run begin
      S.create ()   >>= fun t ->
      S.list t path >>= fun paths ->
      List.iter (fun p -> IrminLog.msg "%s" (IrminTree.Path.pretty p)) paths;
      return_unit
    end
  in
  Term.(pure ls $ store $ path),
  Term.info "ls" ~doc ~man

let tree_doc = "List the store contents."
let tree =
  let doc = tree_doc in
  let man = [
    `S "DESCRIPTION";
    `P tree_doc;
  ] in
  let tree (module S: Irmin.SIMPLE) =
    run begin
      S.create () >>= fun t ->
      S.contents t >>= fun all ->
      let all = List.map (fun (k,v) -> IrminTree.Path.to_string k, S.Value.pretty v) all in
      let max_lenght l =
        List.fold_left (fun len s -> max len (String.length s)) 0 l in
      let k_max = max_lenght (List.map fst all) in
      let v_max = max_lenght (List.map snd all) in
      let pad = 80 + k_max + v_max in
      List.iter (fun (k,v) ->
          let dots = String.make (pad - String.length k - String.length v) '.' in
          IrminLog.msg "%s%s%s" k dots v
        ) all;
      return_unit
    end
  in
  Term.(pure tree $ store),
  Term.info "tree" ~doc ~man

let write_doc = "Write/modify a node."
let write =
  let doc = write_doc in
  let man = [
    `S "DESCRIPTION";
    `P write_doc;
  ] in
  let write (module S: Irmin.SIMPLE) path value =
    run begin
      S.create () >>= fun t ->
      S.update t path value
    end
  in
  Term.(pure write $ store $ path $ value),
  Term.info "write" ~doc ~man

let rm_doc = "Remove a node."
let rm =
  let doc = rm_doc in
  let man = [
    `S "DESCRIPTION";
    `P rm_doc;
  ] in
  let rm (module S: Irmin.SIMPLE) path =
    run begin
      S.create () >>= fun t ->
      S.remove t path
    end
  in
  Term.(pure rm $ store $ path),
  Term.info "rm" ~doc ~man

let todo () =
  failwith "TODO"

let clone_doc = "Clone a remote irminsule store."
let clone =
  let doc = clone_doc in
  let man = [
    `S "DESCRIPTION";
    `P clone_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "clone" ~doc ~man

let pull_doc = "Pull the contents of a remote irminsule store."
let pull =
  let doc = pull_doc in
  let man = [
    `S "DESCRIPTION";
    `P pull_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "pull" ~doc ~man

let push_doc = "Pull the contents of the local store to a remote irminsule store."
let push =
  let doc = push_doc in
  let man = [
    `S "DESCRIPTION";
    `P push_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "push" ~doc ~man

let snapshot_doc = "Snapshot the contents of the store."
let snapshot =
  let doc = snapshot_doc in
  let man = [
    `S "DESCRIPTION";
    `P snapshot_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "snapshot" ~doc ~man

let revert_doc = "Revert the contents of the store to a previous state."
let revert =
  let doc = revert_doc in
  let man = [
    `S "DESCRIPTION";
    `P revert_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "revert" ~doc ~man

let watch_doc = "Watch the contents of a store and be notified on updates."
let watch =
  let doc = watch_doc in
  let man = [
    `S "DESCRIPTION";
    `P watch_doc;
  ] in
  Term.(pure todo $ pure ()),
  Term.info "watch" ~doc ~man

let dump_doc = "Dump the contents of the store as a Graphviz file."
let dump =
  let doc = dump_doc in
  let man = [
    `S "DESCRIPTION";
    `P dump_doc;
  ] in
  let basename =
    let doc =
      Arg.info ~docv:"BASENAME" ~doc:"Basename for the .dot and .png files." [] in
    Arg.(required & pos 0 (some & string) None & doc) in
  let dump (module S: Irmin.SIMPLE) basename =
    run begin
      S.create () >>= fun t ->
      S.output t basename
    end
  in
  Term.(pure dump $ store $ basename),
  Term.info "dump" ~doc ~man

(* HELP *)
let help =
  let doc = "Display help about Irminsule and Irminsule commands." in
  let man = [
    `S "DESCRIPTION";
    `P "Prints help about Irminsule commands.";
    `P "Use `$(mname) help topics' to get the full list of help topics.";
  ] in
  let topic =
    let doc = Arg.info [] ~docv:"TOPIC" ~doc:"The topic to get help on." in
    Arg.(value & pos 0 (some string) None & doc )
  in
  let help man_format cmds topic = match topic with
    | None       -> `Help (`Pager, None)
    | Some topic ->
      let topics = "topics" :: cmds in
      let conv, _ = Arg.enum (List.rev_map (fun s -> (s, s)) topics) in
      match conv topic with
      | `Error e                -> `Error (false, e)
      | `Ok t when t = "topics" -> List.iter print_endline cmds; `Ok ()
      | `Ok t                   -> `Help (man_format, Some t) in
  Term.(ret (pure help $Term.man_format $Term.choice_names $topic)),
  Term.info "help" ~doc ~man

let default =
  let doc = "Irminsule, the database that never forgets." in
  let man = [
    `S "DESCRIPTION";
    `P "Irminsule is a distributed database with built-in snapshot, branch \
        and revert mechanisms. It is designed to use a large variety of backends, \
        although it is optimized for append-only ones.";
    `P "Irminsule is written in pure OCaml, and can thus be compiled to a variety of \
        backends including Javascript -- to run inside Browsers, and Mirage microkernels \
        -- to run directly on top of Xen.";
    `P "Use either $(b,$(mname) <command> --help) or $(b,$(mname) help <command>) \
        for more information on a specific command.";
  ] in
  let usage _ =
    Printf.printf
      "usage: irmin [--version]\n\
      \             [--help]\n\
      \             <command> [<args>]\n\
      \n\
      The most commonly used irminsule commands are:\n\
      \    init        %s\n\
      \    read        %s\n\
      \    write       %s\n\
      \    rm          %s\n\
      \    ls          %s\n\
      \    tree        %s\n\
      \    clone       %s\n\
      \    pull        %s\n\
      \    push        %s\n\
      \    snaphsot    %s\n\
      \    revert      %s\n\
      \    watch       %s\n\
      \    dump        %s\n\
      \n\
      See `irmin help <command>` for more information on a specific command.\n%!"
      init_doc read_doc write_doc rm_doc ls_doc tree_doc
      clone_doc pull_doc push_doc snapshot_doc revert_doc
      watch_doc dump_doc in
  Term.(pure usage $ (pure ())),
  Term.info "irmin"
    ~version:IrminVersion.current
    ~sdocs:global_option_section
    ~doc
    ~man

let commands = [
  init;
  read;
  write;
  rm;
  ls;
  tree;
  clone;
  pull;
  push;
  snapshot;
  revert;
  watch;
  dump;
]
