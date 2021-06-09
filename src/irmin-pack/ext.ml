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

open! Import
module IO = IO.Unix

module Maker
    (V : Version.S)
    (Config : Conf.S)
    (Node : Irmin.Private.Node.Maker)
    (Commit : Irmin.Private.Commit.Maker) =
struct
  type endpoint = unit
  type info = Commit.Info.t

  module Make
      (M : Irmin.Metadata.S)
      (C : Irmin.Contents.S)
      (P : Irmin.Path.S)
      (B : Irmin.Branch.S)
      (H : Irmin.Hash.S) =
  struct
    module Index = Pack_index.Make (H)
    module Pack = Content_addressable.Maker (V) (Index) (H)
    module Dict = Pack_dict.Make (V)

    module X = struct
      module Hash = H
      module Info = Commit.Info

      type 'a value = { hash : H.t; magic : char; v : 'a } [@@deriving irmin]

      module Contents = struct
        module CA = struct
          module CA_Pack = Pack.Make (struct
            include C
            module H = Irmin.Hash.Typed (H) (C)

            let hash = H.hash
            let magic = 'B'
            let value = value_t C.t
            let encode_value = Irmin.Type.(unstage (encode_bin value))
            let decode_value = Irmin.Type.(unstage (decode_bin value))

            let encode_bin ~dict:_ ~offset:_ v hash =
              encode_value { magic; hash; v }

            let decode_bin ~dict:_ ~hash:_ s off =
              let _, t = decode_value s off in
              t.v

            let magic _ = magic
          end)

          include Content_addressable.Closeable (CA_Pack)
        end

        include Irmin.Contents.Store (CA) (H) (C)
      end

      module Node = struct
        module Node = Node.Make (H) (P) (M)
        module CA = Inode.Make (Config) (H) (Pack) (Node)
        include Irmin.Private.Node.Store (Contents) (CA) (H) (CA.Val) (M) (P)
      end

      module Commit = struct
        module Commit = Commit.Make (H)

        module CA = struct
          module CA_Pack = Pack.Make (struct
            include Commit
            module H = Irmin.Hash.Typed (H) (Commit)

            let hash = H.hash
            let value = value_t Commit.t
            let magic = 'C'
            let encode_value = Irmin.Type.(unstage (encode_bin value))
            let decode_value = Irmin.Type.(unstage (decode_bin value))

            let encode_bin ~dict:_ ~offset:_ v hash =
              encode_value { magic; hash; v }

            let decode_bin ~dict:_ ~hash:_ s off =
              let _, v = decode_value s off in
              v.v

            let magic _ = magic
          end)

          include Content_addressable.Closeable (CA_Pack)
        end

        include Irmin.Private.Commit.Store (Info) (Node) (CA) (H) (Commit)
      end

      module Branch = struct
        module Key = B
        module Val = H
        module AW = Atomic_write.Make (V) (Key) (Val)
        include Atomic_write.Closeable (AW)
      end

      module Slice = Irmin.Private.Slice.Make (Contents) (Node) (Commit)
      module Remote = Irmin.Private.Remote.None (H) (B)

      module Repo = struct
        type t = {
          config : Irmin.Private.Conf.t;
          contents : read Contents.CA.t;
          node : read Node.CA.t;
          commit : read Commit.CA.t;
          branch : Branch.t;
          index : Index.t;
        }

        let contents_t t : 'a Contents.t = t.contents
        let node_t t : 'a Node.t = (contents_t t, t.node)
        let commit_t t : 'a Commit.t = (node_t t, t.commit)
        let branch_t t = t.branch

        let batch t f =
          Commit.CA.batch t.commit (fun commit ->
              Node.CA.batch t.node (fun node ->
                  Contents.CA.batch t.contents (fun contents ->
                      let contents : 'a Contents.t = contents in
                      let node : 'a Node.t = (contents, node) in
                      let commit : 'a Commit.t = (node, commit) in
                      f contents node commit)))

        let unsafe_v config =
          let root = Conf.root config in
          let fresh = Conf.fresh config in
          let lru_size = Conf.lru_size config in
          let readonly = Conf.readonly config in
          Printf.eprintf "RO??? %b\n%!" readonly;
          let log_size = Conf.index_log_size config in
          let throttle = Conf.merge_throttle config in
          let f = ref (fun () -> ()) in
          Printf.eprintf "aaa\n%!";
          let index =
            Index.v
              ~flush_callback:(fun () -> !f ())
                (* backpatching to add pack flush before an index flush *)
              ~fresh ~readonly ~throttle ~log_size root
          in
          Printf.eprintf "bbb\n%!";
          let* contents =
            Contents.CA.v ~fresh ~readonly ~lru_size ~index root
          in
          let* node = Node.CA.v ~fresh ~readonly ~lru_size ~index root in
          let* commit = Commit.CA.v ~fresh ~readonly ~lru_size ~index root in
          let+ branch = Branch.v ~fresh ~readonly root in
          (* Stores share instances in memory, one flush is enough. In case of a
             system crash, the flush_callback might not make with the disk. In
             this case, when the store is reopened, [integrity_check] needs to be
             called to repair the store. *)
          (f := fun () -> Contents.CA.flush ~index:false contents);
          { contents; node; commit; branch; config; index }

        let close t =
          Index.close t.index;
          Contents.CA.close (contents_t t) >>= fun () ->
          Node.CA.close (snd (node_t t)) >>= fun () ->
          Commit.CA.close (snd (commit_t t)) >>= fun () -> Branch.close t.branch

        let v config =
          Lwt.catch
            (fun () -> unsafe_v config)
            (function
              | Version.Invalid { expected; found } as e
                when expected = V.version ->
                  Log.err (fun m ->
                      m "[%s] Attempted to open store of unsupported version %a"
                        (Conf.root config) Version.pp found);
                  Lwt.fail e
              | e -> Lwt.fail e)

        (** Stores share instances in memory, one sync is enough. However each
            store has its own lru and all have to be cleared. *)
        let sync t =
          let on_generation_change () =
            Node.CA.clear_caches (snd (node_t t));
            Commit.CA.clear_caches (snd (commit_t t))
          in
          Contents.CA.sync ~on_generation_change (contents_t t)

        (** Stores share instances so one clear is enough. *)
        let clear t = Contents.CA.clear (contents_t t)

        let flush t =
          Contents.CA.flush (contents_t t);
          Branch.flush t.branch

        module Reconstruct_index = struct
          let pp_hash = Irmin.Type.pp Hash.t

          let decode_contents =
            Irmin.Type.(unstage (decode_bin (value_t Contents.Val.t)))

          let decode_commit =
            Irmin.Type.(unstage (decode_bin (value_t Commit.Val.t)))

          let decode_key = Irmin.Type.(unstage (decode_bin Hash.t))
          let decode_magic = Irmin.Type.(unstage (decode_bin char))

          let decode_buffer ~progress ~total pack dict index =
            let decode_len buf magic =
              try
                let len =
                  match magic with
                  | 'B' -> decode_contents buf 0 |> fst
                  | 'C' -> decode_commit buf 0 |> fst
                  | 'N' | 'I' ->
                      let hash off =
                        let buf =
                          IO.read_buffer ~chunk:Hash.hash_size ~off pack
                        in
                        decode_key buf 0 |> snd
                      in
                      let dict = Dict.find dict in
                      Node.CA.decode_bin ~hash ~dict buf 0
                  | _ -> failwith "unexpected magic char"
                in
                Some len
              with
              | Invalid_argument msg when msg = "index out of bounds" -> None
              | Invalid_argument msg
                when msg = "String.blit / Bytes.blit_string" ->
                  None
            in
            let decode_entry buf off =
              let off_k, k = decode_key buf 0 in
              assert (off_k = Hash.hash_size);
              let off_m, magic = decode_magic buf off_k in
              assert (off_m = Hash.hash_size + 1);
              match decode_len buf magic with
              | Some len ->
                  let new_off = off ++ Int63.of_int len in
                  Log.debug (fun l ->
                      l "k = %a (off, len, magic) = (%a, %d, %c)" pp_hash k
                        Int63.pp off len magic);
                  Index.add index k (off, len, magic);
                  progress (Int63.of_int len);
                  Some new_off
              | None -> None
            in
            let rec read_and_decode ?(retries = 1) off =
              Log.debug (fun l ->
                  l "read_and_decode retries %d off %a" retries Int63.pp off);
              let chunk = 64 * 10 * retries in
              let buf = IO.read_buffer ~chunk ~off pack in
              match decode_entry buf off with
              | Some new_off -> new_off
              | None ->
                  (* the biggest entry in a tezos store is a blob of 54801B *)
                  if retries > 90 then
                    failwith
                      "too many retries to read data, buffer size = 57600B"
                  else (read_and_decode [@tailcall]) ~retries:(retries + 1) off
            in
            let rec read_buffer off =
              if off >= total then ()
              else
                let new_off = read_and_decode off in
                (read_buffer [@tailcall]) new_off
            in
            read_buffer Int63.zero

          let reconstruct ?output config =
            if Conf.readonly config then raise S.RO_not_allowed;
            Log.info (fun l -> l "[%s] reconstructing index" (Conf.root config));
            let root = Conf.root config in
            let dest = match output with Some path -> path | None -> root in
            let log_size = Conf.index_log_size config in
            let index = Index.v ~fresh:true ~readonly:false ~log_size dest in
            let pack_file = Filename.concat root "store.pack" in
            let pack =
              IO.v ~fresh:false ~readonly:true ~version:(Some V.version)
                pack_file
            in
            let dict = Dict.v ~fresh:false ~readonly:true root in
            let total = IO.offset pack in
            let bar, progress =
              Utils.Progress.counter ~total ~sampling_interval:100
                ~message:"Reconstructing index" ~pp_count:Utils.pp_bytes ()
            in
            decode_buffer ~progress ~total pack dict index;
            Index.close index;
            IO.close pack;
            Dict.close dict;
            Utils.Progress.finalise bar
        end
      end
    end

    let integrity_check ?ppf ~auto_repair t =
      let module Checks = Checks.Index (Index) in
      let contents = X.Repo.contents_t t in
      let nodes = X.Repo.node_t t |> snd in
      let commits = X.Repo.commit_t t |> snd in
      let check ~kind ~offset ~length k =
        match kind with
        | `Contents -> X.Contents.CA.integrity_check ~offset ~length k contents
        | `Node -> X.Node.CA.integrity_check ~offset ~length k nodes
        | `Commit -> X.Commit.CA.integrity_check ~offset ~length k commits
      in
      Checks.integrity_check ?ppf ~auto_repair ~check t.index

    include Irmin.Of_private (X)

    let integrity_check_inodes ?heads t =
      Log.debug (fun l -> l "Check integrity for inodes");
      let bar, (_, progress_nodes, progress_commits) =
        Utils.Progress.increment ()
      in
      let errors = ref [] in
      let nodes = X.Repo.node_t t |> snd in
      let node k =
        progress_nodes ();
        X.Node.CA.integrity_check_inodes nodes k >|= function
        | Ok () -> ()
        | Error msg -> errors := msg :: !errors
      in
      let commit _ =
        progress_commits ();
        Lwt.return_unit
      in
      let* heads =
        match heads with None -> Repo.heads t | Some m -> Lwt.return m
      in
      let hashes = List.map (fun x -> `Commit (Commit.hash x)) heads in
      let+ () =
        Repo.iter ~cache_size:1_000_000 ~min:[] ~max:hashes ~node ~commit t
      in
      Utils.Progress.finalise bar;
      let pp_commits = Fmt.list ~sep:Fmt.comma Commit.pp_hash in
      if !errors = [] then
        Fmt.kstrf (fun x -> Ok (`Msg x)) "Ok for heads %a" pp_commits heads
      else
        Fmt.kstrf
          (fun x -> Error (`Msg x))
          "Inconsistent inodes found for heads %a: %a" pp_commits heads
          Fmt.(list ~sep:comma string)
          !errors

    module Stats = struct
      type paths = step option list list

      type stat = {
        length : int * paths;
        width : int * paths;
        mp : int * paths;
      }

      let pp_hash = Irmin.Type.pp Hash.t

      let traverse_inodes commit repo =
        let visited = Hashtbl.create 100 in
        let get k =
          try Hashtbl.find visited k
          with Not_found ->
            { length = (0, []); width = (0, []); mp = (0, []) }
        in
        let get_root k =
          try Hashtbl.find visited k
          with Not_found ->
            { length = (0, [ [] ]); width = (0, [ [] ]); mp = (0, [ [] ]) }
        in
        let max_length = ref 0 in
        let max_mp = ref 0 in
        let max_width = ref 0 in
        let discard k =
          let stat = get k in
          if
            fst stat.length < !max_length
            && fst stat.mp < !max_mp
            && fst stat.width < !max_width
          then Hashtbl.remove visited k
          else (
            if fst stat.length > !max_length then max_length := fst stat.length;
            if fst stat.mp > !max_mp then max_mp := fst stat.mp;
            if fst stat.width > !max_width then max_width := fst stat.width;
            Hashtbl.filter_map_inplace
              (fun _ stat' ->
                if
                  fst stat'.mp < !max_mp
                  && fst stat'.mp < !max_mp
                  && fst stat'.width < !max_width
                then None
                else Some stat')
              visited)
        in
        let pred_node repo k =
          let stat_k = get_root k in
          let update_stat x step stat paths =
            if fst stat > x then stat
            else
              let paths = List.map (fun rev_path -> step :: rev_path) paths in
              if fst stat < x then (x, paths)
              else
                let paths' = paths @ snd stat in
                (fst stat, paths')
          in
          (* Update the stats and the paths for node n. *)
          let visit nb_siblings step n =
            let stat_n = get n in
            let length, width =
              match step with
              | None ->
                  (* Do not update length if n is an inode. *)
                  (stat_k.length, stat_k.width)
              | Some _ ->
                  let length = fst stat_k.length + 1 in
                  let length =
                    update_stat length step stat_n.length (snd stat_k.length)
                  in
                  (* Only update the path, the width is determined when the node
                     is traversed. *)
                  let width =
                    update_stat 0 step stat_n.width (snd stat_k.width)
                  in
                  (length, width)
            in
            let mp = fst stat_k.mp + nb_siblings in
            let mp = update_stat mp step stat_n.mp (snd stat_k.mp) in
            let stat_n' = { length; width; mp } in
            Hashtbl.replace visited n stat_n'
          in
          X.Node.find (X.Repo.node_t repo) k >|= function
          | None -> Fmt.failwith "hash %a not found" pp_hash k
          | Some v ->
              let width = X.Node.Val.length v in
              let nb_children = X.Node.CA.Val.nb_children v in
              let visit = visit nb_children in
              let preds =
                List.rev_map
                  (function
                    | s, `Inode x ->
                        assert (s = None);
                        visit s x;
                        `Node x
                    | s, `Node x ->
                        visit s x;
                        `Node x
                    | s, `Contents x ->
                        visit s x;
                        `Contents x)
                  (X.Node.CA.Val.pred v)
              in
              (* Once we updated its preds we can remove the node from the
                 table, if it's not a max width. If its a max width, we update
                 the width. *)
              if width < !max_width then Hashtbl.remove visited k
              else (
                max_width := width;
                let width = (width, snd stat_k.width) in
                Hashtbl.replace visited k { stat_k with width });
              preds
        in
        (* We are traversing only one commit. *)
        let pred_commit repo k =
          X.Commit.find (X.Repo.commit_t repo) k >|= function
          | None -> []
          | Some c ->
              let node = X.Commit.Val.node c in
              [ `Node node ]
        in
        (* Keep only the contents that have max length, mp or width. *)
        let pred_contents _repo k =
          discard k;
          Lwt.return []
        in
        (* We want to discover all paths to a node, so we don't cache nodes
           during traversal. *)
        let* () =
          Repo.breadth_first_traversal ~cache_size:0 ~pred_node ~pred_commit
            ~pred_contents ~max:[ commit ] repo
        in
        let l, w, m =
          Hashtbl.fold
            (fun _ stats (l, w, m) ->
              let l =
                if fst stats.length = !max_length then
                  let length = snd stats.length |> List.map List.rev in
                  List.rev_append length l
                else l
              in
              let w =
                if fst stats.width = !max_width then
                  let width = snd stats.width |> List.map List.rev in
                  List.rev_append width w
                else w
              in
              let m =
                if fst stats.mp = !max_mp then
                  let mp = snd stats.mp |> List.map List.rev in
                  List.rev_append mp m
                else m
              in
              (l, w, m))
            visited ([], [], [])
        in
        let max_length = (!max_length, l) in
        let max_wide = (!max_width, w) in
        let max_mp = (!max_mp, m) in
        Lwt.return { length = max_length; width = max_wide; mp = max_mp }

      let run ~commit repo =
        let hash = `Commit (Commit.hash commit) in
        traverse_inodes hash repo
    end

    let sync = X.Repo.sync
    let clear = X.Repo.clear
    let migrate = Migrate.run
    let flush = X.Repo.flush
    let reconstruct_index = X.Repo.Reconstruct_index.reconstruct
  end
end
