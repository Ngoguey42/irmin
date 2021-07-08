open! Import
module IO = IO.Unix

module type Args = sig
  module Version : Version.S
  module Hash : Irmin.Hash.S
  module Index : Pack_index.S with type key := Hash.t
  module Inode_internal : Inode.Internal with type hash := Hash.t

  module Inode :
    Inode.S with type key := Hash.t with type value = Inode_internal.Val.t

  module Dict : Pack_dict.S
  module Contents : Pack_value.S with type hash = Hash.t
  module Commit_value : Pack_value.S with type hash = Hash.t

  module Commit :
    Irmin.Commit.S with type hash = Hash.t with type t = Commit_value.t

  module Info : Irmin.Info.S with type t = Commit.Info.t
end

let mem_usage ppf =
  Format.fprintf ppf "%.3fGB"
    (Gc.((quick_stat ()).heap_words) * (Sys.word_size / 8)
    |> float_of_int
    |> ( *. ) 1e-9)

let add_to_assoc_at_key (table : (_, (_ * _) list) Hashtbl.t) k0 k1 v =
  let l = match Hashtbl.find_opt table k0 with None -> [] | Some l -> l in
  Hashtbl.replace table k0 ((k1, v) :: l)

module Make (Args : Args) : sig
  val run : Irmin.config -> Format.formatter -> unit
end = struct
  open Args

  let pp_key = Irmin.Type.pp Hash.t
  let key_equal = Irmin.Type.(unstage (equal Hash.t))
  let decode_key = Irmin.Type.(unstage (decode_bin Hash.t))
  let decode_kind = Irmin.Type.(unstage (decode_bin Pack_value.Kind.t))

  (* [Repr] doesn't yet support buffered binary decoders, so we hack one
     together by re-interpreting [Invalid_argument _] exceptions from [Repr]
     as requests for more data. *)
  exception Not_enough_buffer

  type offset = int63 [@@deriving irmin]
  type length = int [@@deriving irmin]
  type hash = Hash.t [@@deriving irmin]
  type kind = char [@@deriving irmin]
  type ('k, 'v) assoc = ('k * 'v) list [@@deriving irmin]

  module Index = struct
    include Index

    let value_t : value Irmin.Type.t =
      Irmin.Type.(triple int63_t int Pack_value.Kind.t)
  end

  module Offsetmap = struct
    include Stdlib.Map.Make (Int63)

    let key_t = offset_t

    type 'v location =
      [ `Above of key * 'v
      | `Below of key * 'v
      | `Between of (key * 'v) * (key * 'v)
      | `Empty
      | `Exact of key * 'v ]
    [@@deriving irmin]

    let locate map k : _ location =
      let closest_above = find_first_opt (fun k' -> k' >= k) map in
      let closest_below = find_last_opt (fun k' -> k' <= k) map in
      match (closest_below, closest_above) with
      | Some ((k0, _) as t0), Some (k1, _) when k0 = k1 -> `Exact t0
      | Some t0, Some t1 -> `Between (t0, t1)
      | Some t0, None -> `Above t0
      | None, Some t1 -> `Below t1
      | None, None -> `Empty
  end

  module Datemap = Stdlib.Map.Make (Int64)

  let _ = ignore (Offsetmap.locate, Index.value_t, key_equal)

  module Offsetgraph = Graph.Imperative.Digraph.Concrete (struct
    type t = offset

    let compare = Int63.compare
    let equal = ( = )
    let hash v = Irmin.Type.(short_hash Int63.t |> unstage) v
  end)

  module Hashgraph = Graph.Imperative.Digraph.Concrete (struct
    type t = Hash.t * [ `Inner | `Outer ]
    [@@deriving irmin ~compare ~equal ~short_hash]

    let hash x = short_hash x
  end)

  (** Index pack file *)
  module Pass0 = struct
    type entry = { len : length; kind : kind }

    type content = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let decode_entry_length = function
      | Pack_value.Kind.Contents -> Contents.decode_bin_length
      | Commit -> Commit_value.decode_bin_length
      | Node | Inode -> Inode.decode_bin_length

    let decode_entry_exn ~off ~buffer ~buffer_off =
      try
        let off_after_key, key = decode_key buffer buffer_off in
        assert (off_after_key = buffer_off + Hash.hash_size);
        let off_after_kind, kind = decode_kind buffer off_after_key in
        assert (off_after_kind = buffer_off + Hash.hash_size + 1);
        let len = decode_entry_length kind buffer buffer_off in
        (off, len, kind, key)
      with
      | Invalid_argument msg when msg = "index out of bounds" ->
          raise Not_enough_buffer
      | Invalid_argument msg when msg = "String.blit / Bytes.blit_string" ->
          raise Not_enough_buffer

    let fold_entries ~total pack f acc0 =
      let buffer = ref (Bytes.create (1024 * 1024)) in
      let refill_buffer ~from =
        let read = IO.read pack ~off:from !buffer in
        let filled = read = Bytes.length !buffer in
        let eof = Int63.equal total (Int63.add from (Int63.of_int read)) in
        if (not filled) && not eof then
          `Error
            (Fmt.str
               "When refilling from offset %#Ld (total %#Ld), read %#d but \
                expected %#d"
               (Int63.to_int64 from) (Int63.to_int64 total) read
               (Bytes.length !buffer))
        else `Ok
      in
      let expand_and_refill_buffer ~from =
        let length = Bytes.length !buffer in
        if length > 1_000_000_000 (* 1 GB *) then
          `Error
            (Fmt.str
               "Couldn't decode the value at offset %a in %d of buffer space. \
                Corrupted data file?"
               Int63.pp from length)
        else (
          buffer := Bytes.create (2 * length);
          refill_buffer ~from)
      in
      let rec aux ~buffer_off off acc =
        assert (off <= total);
        if off = total then `Eof acc
        else
          match
            decode_entry_exn ~off
              ~buffer:(Bytes.unsafe_to_string !buffer)
              ~buffer_off
          with
          | entry ->
              let _, entry_len, _, _ = entry in
              let entry_lenL = Int63.of_int entry_len in
              aux ~buffer_off:(buffer_off + entry_len) (off ++ entry_lenL)
                (f acc entry)
          | exception Not_enough_buffer -> (
              let res =
                if buffer_off > 0 then
                  (* Try again with the value at the start of the buffer. *)
                  refill_buffer ~from:off
                else
                  (* The entire buffer isn't enough to hold this value: expand it. *)
                  expand_and_refill_buffer ~from:off
              in
              match res with
              | `Ok -> aux ~buffer_off:0 off acc
              | `Error msg -> `Leftovers (acc, msg))
      in
      refill_buffer ~from:Int63.zero |> ignore;
      aux ~buffer_off:0 Int63.zero acc0

    let run ~progress ~total pack =
      let per_hash = Hashtbl.create 10_000_000 in
      let acc0 = (0, Offsetmap.empty) in
      let accumulate (idx, per_offset) (off, len, kind, key) =
        progress (Int63.of_int len);
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%a', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (Int63.to_float off /. Int63.to_float total *. 100.)
            Pack_value.Kind.pp kind pp_key key len mem_usage;
        add_to_assoc_at_key per_hash key off
          { len; kind = Pack_value.Kind.to_magic kind };
        let per_offset = Offsetmap.add off key per_offset in
        (idx + 1, per_offset)
      in
      let res = fold_entries ~total pack accumulate acc0 in
      let (_, per_offset), extra_errors =
        match res with
        | `Eof acc -> (acc, [])
        | `Leftovers (acc, err) -> (acc, [ err ])
      in
      {
        per_offset;
        per_hash;
        extra_errors;
        entry_count = Offsetmap.cardinal per_offset;
      }
  end

  (** Partially rebuild values *)
  module Pass1 = struct
    type value =
      [ `Blob of Contents.t
      | `Node of Inode_internal.Raw.t * (hash * offset) list
      | `Commit of Commit_value.t ]

    type entry = {
      len : length;
      kind : kind;
      details : [ `Error_p1 of string | `Ok of value ];
    }

    type content = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let fold_entries pack ~pack_size pass0 f acc =
      let buffer = ref (Bytes.create (1024 * 1024)) in
      let refill_buffer ~from =
        let read = IO.read pack ~off:from !buffer in
        let filled = read = Bytes.length !buffer in
        let eof = Int63.equal pack_size (Int63.add from (Int63.of_int read)) in
        assert (filled || eof);
        if not filled then buffer := Bytes.sub !buffer 0 read
      in
      let expand_and_refill_buffer ~from length =
        buffer := Bytes.create length;
        refill_buffer ~from
      in
      let ensure_loaded buffer_off ~from ~len =
        if Bytes.length !buffer < len then (
          expand_and_refill_buffer ~from len;
          0)
        else if buffer_off + len > Bytes.length !buffer then (
          refill_buffer ~from;
          0)
        else buffer_off
      in
      let aux off key (buffer_off, acc) =
        let Pass0.{ len; kind } =
          Hashtbl.find pass0.Pass0.per_hash key |> List.assoc off
        in
        let buffer_off = ensure_loaded buffer_off ~from:off ~len in
        let acc = f acc key off len kind !buffer buffer_off in
        (buffer_off + len, acc)
      in
      refill_buffer ~from:Int63.zero;
      Offsetmap.fold aux pass0.Pass0.per_offset (0, acc) |> snd

    let run ~progress pack ~pack_size dict pass0 =
      let entry_count = Offsetmap.cardinal pass0.Pass0.per_offset in
      let per_hash = Hashtbl.create entry_count in
      let f idx key off len kind buffer buffer_off =
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%c', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (float_of_int idx /. float_of_int entry_count *. 100.)
            kind pp_key key len mem_usage;
        let reconstruct_commit () =
          Commit_value.decode_bin
            ~dict:(fun _ -> assert false)
            ~hash:(fun _ -> assert false)
            (Bytes.unsafe_to_string buffer)
            buffer_off
          |> snd
        in
        let reconstruct_contents () =
          Contents.decode_bin
            ~dict:(fun _ -> assert false)
            ~hash:(fun _ -> assert false)
            (Bytes.unsafe_to_string buffer)
            buffer_off
          |> snd
        in
        let reconstruct_node () =
          let indirect_children = ref [] in
          let hash_of_offset o =
            match Offsetmap.find_opt o pass0.Pass0.per_offset with
            | None ->
                Fmt.failwith "Could not find child at offset %a"
                  (Repr.pp Int63.t) o
            | Some key ->
                indirect_children := (key, o) :: !indirect_children;
                key
          in
          let bin =
            Inode_internal.Raw.decode_bin ~dict:(Dict.find dict)
              ~hash:hash_of_offset
              (Bytes.unsafe_to_string buffer)
              buffer_off
            |> snd
          in
          (bin, !indirect_children)
        in
        let details =
          try
            match kind with
            | 'C' -> `Ok (`Commit (reconstruct_commit ()))
            | 'B' -> `Ok (`Blob (reconstruct_contents ()))
            | 'I' | 'N' -> `Ok (`Node (reconstruct_node ()))
            | _ -> assert false
          with
          | Assert_failure _ as e -> raise e
          | e -> `Error_p1 (Printexc.to_string e)
        in
        add_to_assoc_at_key per_hash key off { len; kind; details };
        progress Int63.one;
        idx + 1
      in

      let (_ : int) = fold_entries pack ~pack_size pass0 f 0 in
      {
        entry_count = pass0.Pass0.entry_count;
        per_hash;
        per_offset = pass0.Pass0.per_offset;
        extra_errors = pass0.Pass0.extra_errors;
      }
  end

  (** Reconstruct inodes *)
  module Pass2 = struct
    type value =
      [ `Blob of Contents.t
      | `Node of Inode.Val.t * (hash * offset) list
      | `Commit of Commit_value.t ]

    type entry = {
      len : length;
      kind : kind;
      details :
        [ `Error_p1 of string
        | `Error_p2 of Inode_internal.Raw.t * string
        | `Ok of value ];
    }

    type content = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let run ~progress pass1 =
      let entry_count = Offsetmap.cardinal pass1.Pass1.per_offset in
      let obj_of_hash = Hashtbl.create entry_count in
      let per_hash = Hashtbl.create entry_count in

      let get_raw_inode key =
        match Hashtbl.find_opt pass1.Pass1.per_hash key with
        | Some Pass1.[ (_, { details = `Ok (`Node (bin, _)); _ }) ] -> Some bin
        | Some Pass1.[ (_, { details = `Error_p1 _; _ }) ] ->
            Fmt.failwith
              "Abort stable inode rehash because a children failed at Pass1  \
               (%a)"
              (Repr.pp Hash.t) key
        | Some Pass1.[ (_, { details = `Ok (`Commit _ | `Blob _); _ }) ] ->
            Fmt.failwith
              "Abort stable inode rehash because a children is a commit or a \
               blob (%a)"
              (Repr.pp Hash.t) key
        | Some l -> (
            match
              List.find_map
                (function
                  | _, Pass1.{ details = `Ok (`Node (bin, _)); _ } -> Some bin
                  | _ -> None)
                l
            with
            | Some bin -> Some bin (* /!\ Using the first occurence of hash *)
            | None ->
                Fmt.failwith
                  "Abort stable inode rehash because a children hash occurs \
                   multiple time and none are good inodes (%a)"
                  (Repr.pp Hash.t) key)
        | None ->
            Fmt.failwith
              "Abort stable inode rehash because a children hash is unknown \
               (%a)"
              (Repr.pp Hash.t) key
      in

      let reconstruct_inode key bin =
        let obj = Inode_internal.Val.of_raw get_raw_inode bin in
        let key' = Inode_internal.Val.rehash obj in
        if not (key_equal key key') then
          Fmt.failwith
            "Rehasing inode have a different hash. Expected %a, found %a" pp_key
            key pp_key key' obj;
        obj
      in

      let aux off key idx =
        let Pass1.{ len; kind; details } =
          Hashtbl.find pass1.Pass1.per_hash key |> List.assoc off
        in
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%c', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (float_of_int idx /. float_of_int entry_count *. 100.)
            kind pp_key key len mem_usage;
        let details =
          match details with
          | `Ok (`Node (bin, indirect_children)) -> (
              try
                let obj = reconstruct_inode key bin in
                `Ok (`Node (obj, indirect_children))
              with
              | Assert_failure _ as e -> raise e
              | e ->
                  (* `Error_p2 (bin, Printexc.to_string e) *)
                  raise e)
          | (`Ok (`Blob _ | `Commit _) as v) | (`Error_p1 _ as v) -> v
        in

        add_to_assoc_at_key per_hash key off { len; kind; details };
        progress Int63.one;
        idx + 1
      in

      let (_ : int) = Offsetmap.fold aux pass1.Pass1.per_offset 0 in
      {
        entry_count = pass1.Pass1.entry_count;
        per_hash;
        per_offset = pass1.Pass1.per_offset;
        extra_errors = pass1.Pass1.extra_errors;
      }
  end

  (** Build a graph of all this.

      Commits are not linked together. *)
  module Pass3 = struct
    type value =
      [ `Blob of Contents.t | `Node of Inode.Val.t | `Commit of Commit_value.t ]

    type entry = {
      len : length;
      kind : kind;
      details :
        [ `Error_p1 of string
        | `Error_p2 of Inode_internal.Raw.t * string
        | `Error_p3 of value * string
        | `Ok of value ];
    }

    type t = {
      entry_count : int;
      graph : Hashgraph.t;
      commits_per_date : Hash.t Datemap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
    }

    let run ~progress pass2 =
      let entry_count = Offsetmap.cardinal pass2.Pass2.per_offset in
      let graph = Hashgraph.create ~size:entry_count () in
      let commits_per_date = ref Datemap.empty in
      let per_hash = Hashtbl.create entry_count in
      let aux off key idx =
        let assoc = Hashtbl.find pass2.Pass2.per_hash key in
        let multiple_hash_occurences = List.length assoc > 1 in
        let Pass2.{ len; kind; details } = List.assoc off assoc in
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%c', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (float_of_int idx /. float_of_int entry_count *. 100.)
            kind pp_key key len mem_usage;

        let details =
          match details with
          | (`Error_p1 _ | `Error_p2 _) as x -> x
          | `Ok obj -> (
              let obj_out =
                match obj with
                | (`Commit _ | `Blob _) as x -> x
                | `Node (x, _) -> `Node x
              in
              let error_p3 e = `Error_p3 (obj_out, e) in
              if multiple_hash_occurences then
                Fmt.str
                  "Could not link hash (%a) because it has several occurences"
                  pp_key key
                |> error_p3
              else
                let errors = ref [] in
                let ok_or_errors () =
                  match !errors with
                  | [] -> `Ok obj_out
                  | l -> String.concat "\n" l |> error_p3
                in
                let link_to_pred key' =
                  if Hashtbl.mem pass2.Pass2.per_hash key' then
                    Hashgraph.add_edge graph (key, `Inner) (key', `Outer)
                  else
                    let e = Fmt.str "Unknown children hash %a" pp_key key' in
                    errors := e :: !errors
                in

                match obj with
                | `Blob _ as x ->
                    Hashgraph.add_edge graph (key, `Outer) (key, `Inner);
                    ok_or_errors ()
                | `Commit c as x ->
                    link_to_pred (Commit.node c);
                    let date = Commit.info c |> Info.date in
                    commits_per_date := Datemap.add date key !commits_per_date;
                    ok_or_errors ()
                | `Node (n, indirect_children) as x ->
                    Hashgraph.add_edge graph (key, `Outer) (key, `Inner);
                    let children =
                      Inode.Val.pred n
                      |> List.map (function
                             | `Contents h | `Inode h | `Node h -> h)
                    in
                    List.iter link_to_pred children;
                    let direct_children =
                      List.filter
                        (fun k -> not (List.mem_assoc k indirect_children))
                        children
                    in
                    List.iter
                      (fun (k, o) ->
                        if o >= off then
                          let e =
                            Fmt.str "Children %a with higher offset %a (+%a)"
                              pp_key k (Repr.pp Int63.t) o (Repr.pp Int63.t)
                              Int63.(sub o off)
                          in
                          errors := e :: !errors)
                      indirect_children;
                    List.iter
                      (fun k ->
                        let e = Fmt.str "Children by hash %a" pp_key k in
                        errors := e :: !errors)
                      direct_children;
                    ok_or_errors ())
        in
        add_to_assoc_at_key per_hash key off { len; kind; details };

        progress Int63.one;
        idx + 1
      in

      let (_ : int) = Offsetmap.fold aux pass2.Pass2.per_offset 0 in
      let commits_per_date = !commits_per_date in
      {
        entry_count = pass2.Pass2.entry_count;
        graph;
        commits_per_date;
        per_hash;
      }
  end

  let run config =
    if Conf.readonly config then raise S.RO_not_allowed;
    let run_duration = Mtime_clock.counter () in
    let root = Conf.root config in
    let log_size = Conf.index_log_size config in
    let pack_file = Filename.concat root "store.pack" in
    Printf.eprintf "opening index\n%!";
    let index = Index.v ~fresh:false ~readonly:true ~log_size root in
    Printf.eprintf "opening pack\n%!";
    let pack =
      IO.v ~fresh:false ~readonly:true ~version:(Some Version.version) pack_file
    in
    Printf.eprintf "opening dict\n%!";
    let dict = Dict.v ~fresh:false ~readonly:true root in
    let byte_count = IO.offset pack in

    let pass0 () =
      Printf.eprintf "Pass0 - go\n%!";
      let bar, progress =
        Utils.Progress.counter ~total:byte_count ~sampling_interval:10_000
          ~message:"Brute force integrity check, pass0" ~pp_count:Utils.pp_bytes
          ()
      in
      let pass0 = Pass0.run ~progress ~total:byte_count pack in
      Utils.Progress.finalise bar;
      Printf.eprintf "Pass0 - done\n%!";
      pass0
    in

    let pass1 () =
      let pass0 = pass0 () in
      Gc.compact ();
      let entry_count = Int63.of_int pass0.Pass0.entry_count in
      Printf.eprintf "Pass1 - go\n%!";
      let bar, progress =
        Utils.Progress.counter ~total:entry_count ~sampling_interval:10_000
          ~message:"Brute force integrity check, pass1" ()
      in
      let pass1 = Pass1.run ~progress ~pack_size:byte_count pack dict pass0 in
      Utils.Progress.finalise bar;
      Printf.eprintf "Pass1 - done\n%!";
      pass1
    in

    let pass2 () =
      let pass1 = pass1 () in
      Gc.compact ();
      let entry_count = Int63.of_int pass1.Pass1.entry_count in
      Printf.eprintf "Pass2 - go\n%!";
      let bar, progress =
        Utils.Progress.counter ~total:entry_count ~sampling_interval:10_000
          ~message:"Brute force integrity check, pass2" ()
      in
      let pass2 = Pass2.run ~progress pass1 in
      Utils.Progress.finalise bar;
      Printf.eprintf "Pass2 - done\n%!";
      pass2
    in

    let pass3 () =
      let pass2 = pass2 () in
      Gc.compact ();
      Printf.eprintf "Pass3 - go\n%!";
      let entry_count = Int63.of_int pass2.Pass2.entry_count in
      let bar, progress =
        Utils.Progress.counter ~total:entry_count ~sampling_interval:10_000
          ~message:"Brute force integrity check, pass3" ()
      in
      let pass3 = Pass3.run ~progress pass2 in
      Utils.Progress.finalise bar;
      Printf.eprintf "Pass3 - done\n%!";
      pass3
    in

    let _ = pass3 () in

    ignore index;
    IO.close pack;
    fun ppf -> Format.fprintf ppf "success!!"
end
