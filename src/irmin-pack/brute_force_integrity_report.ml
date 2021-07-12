(** This takes 200GB RAM and 90 (TODO) minutes for a 5GB pack file. *)

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

  module Datemap = struct
    include Stdlib.Map.Make (Int64)

    (** Available from ocaml 4.12. *)
    let to_rev_seq map = to_seq map |> List.of_seq |> List.rev |> List.to_seq
  end

  let _ = ignore (Offsetmap.locate, Index.value_t, key_equal)

  module Offsetgraph = Graph.Imperative.Digraph.Concrete (struct
    type t = offset

    let compare = Int63.compare
    let equal = ( = )
    let hash v = Irmin.Type.(short_hash Int63.t |> unstage) v
  end)

  module Hashgraph = Graph.Imperative.Digraph.Concrete (struct
    type t = Hash.t [@@deriving irmin ~compare ~equal ~short_hash]

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

    let fold_entries ~byte_count pack f acc0 =
      let buffer = ref (Bytes.create (1024 * 1024)) in
      let refill_buffer ~from =
        let read = IO.read pack ~off:from !buffer in
        let filled = read = Bytes.length !buffer in
        let eof = Int63.equal byte_count (Int63.add from (Int63.of_int read)) in
        if (not filled) && not eof then
          `Error
            (Fmt.str
               "When refilling from offset %#Ld (byte_count %#Ld), read %#d \
                but expected %#d"
               (Int63.to_int64 from)
               (Int63.to_int64 byte_count)
               read (Bytes.length !buffer))
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
        assert (off <= byte_count);
        if off = byte_count then `Eof acc
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

    let run ~progress ~byte_count pack =
      let per_hash = Hashtbl.create 10_000_000 in
      let acc0 = (0, Offsetmap.empty) in
      let accumulate (idx, per_offset) (off, len, kind, key) =
        progress (Int63.of_int len);
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%a', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (Int63.to_float off /. Int63.to_float byte_count *. 100.)
            Pack_value.Kind.pp kind pp_key key len mem_usage;
        add_to_assoc_at_key per_hash key off
          { len; kind = Pack_value.Kind.to_magic kind };
        let per_offset = Offsetmap.add off key per_offset in
        (idx + 1, per_offset)
      in
      let res = fold_entries ~byte_count pack accumulate acc0 in
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
      reconstruction : [ `None | `Some of value ];
      errors : string list;
    }

    type content = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let fold_entries pack ~byte_count pass0 f acc =
      let buffer = ref (Bytes.create (1024 * 1024)) in
      let refill_buffer ~from =
        let read = IO.read pack ~off:from !buffer in
        let filled = read = Bytes.length !buffer in
        let eof = Int63.equal byte_count (Int63.add from (Int63.of_int read)) in
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

    let run ~progress pack ~byte_count dict pass0 =
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
        let reconstruction, errors =
          try
            match kind with
            | 'C' -> (`Some (`Commit (reconstruct_commit ())), [])
            | 'B' -> (`Some (`Blob (reconstruct_contents ())), [])
            | 'I' | 'N' -> (`Some (`Node (reconstruct_node ())), [])
            | _ -> assert false
          with
          | Assert_failure _ as e -> raise e
          | e -> (`None, [ Printexc.to_string e ])
        in
        add_to_assoc_at_key per_hash key off
          { len; kind; reconstruction; errors };
        progress Int63.one;
        idx + 1
      in

      let (_ : int) = fold_entries pack ~byte_count pass0 f 0 in
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
      reconstruction :
        [ `None | `Some of value | `Bin_only of Inode_internal.Raw.t ];
      errors : string list;
    }

    type content = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let run ~progress pass1 =
      let entry_count = Offsetmap.cardinal pass1.Pass1.per_offset in
      let per_hash = Hashtbl.create entry_count in

      let get_raw_inode key =
        match Hashtbl.find_opt pass1.Pass1.per_hash key with
        | Some Pass1.[ (_, { reconstruction = `Some (`Node (bin, _)); _ }) ] ->
            Some bin
        | Some Pass1.[ (_, { reconstruction = `None; _ }) ] ->
            Fmt.failwith
              "Abort stable inode rehash because a children failed at Pass1  \
               (%a)"
              (Repr.pp Hash.t) key
        | Some
            Pass1.[ (_, { reconstruction = `Some (`Commit _ | `Blob _); _ }) ]
          ->
            Fmt.failwith
              "Abort stable inode rehash because a children is a commit or a \
               blob (%a)"
              (Repr.pp Hash.t) key
        | Some l -> (
            match
              List.find_map
                (function
                  | _, Pass1.{ reconstruction = `Some (`Node (bin, _)); _ } ->
                      Some bin
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
        let assoc = Hashtbl.find pass1.Pass1.per_hash key in
        let multiple_hash_occurences = List.length assoc > 1 in
        let Pass1.{ len; kind; reconstruction; errors } =
          List.assoc off assoc
        in
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%c', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (float_of_int idx /. float_of_int entry_count *. 100.)
            kind pp_key key len mem_usage;
        let reconstruction, new_errors =
          match reconstruction with
          | `Some (`Node (bin, indirect_children)) -> (
              try
                let obj = reconstruct_inode key bin in
                (`Some (`Node (obj, indirect_children)), [])
              with
              | Assert_failure _ as e -> raise e
              | e -> (`Bin_only bin, [ Printexc.to_string e ]))
          | (`Some (`Blob _ | `Commit _) as v) | (`None as v) -> (v, [])
        in
        let errors =
          errors
          @ new_errors
          @
          if multiple_hash_occurences then
            [
              Fmt.str "Hash (%a) has %d occurences in the pack file" pp_key key
                (List.length assoc);
            ]
          else []
        in
        add_to_assoc_at_key per_hash key off
          { len; kind; reconstruction; errors };
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

  (** Build a graph and prepare for traversal.

      Commits are not linked together. *)
  module Pass3 = struct
    type value =
      [ `Blob of Contents.t | `Node of Inode.Val.t | `Commit of Commit_value.t ]

    type entry = {
      len : length;
      kind : kind;
      reconstruction :
        [ `None | `Some of value | `Bin_only of Inode_internal.Raw.t ];
      errors : string list;
    }

    type t = {
      entry_count : int;
      graph : Hashgraph.t;
      commits_per_date : Hash.t Datemap.t;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let run ~progress pass2 =
      let entry_count = pass2.Pass2.entry_count in
      let graph = Hashgraph.create ~size:entry_count () in
      let commits_per_date = ref Datemap.empty in
      let per_hash = Hashtbl.create entry_count in
      let aux off key idx =
        let assoc = Hashtbl.find pass2.Pass2.per_hash key in
        let Pass2.{ len; kind; reconstruction; errors } =
          List.assoc off assoc
        in
        if idx mod 2_000_000 = 0 then
          Fmt.epr
            "\n%#12dth at %#13Ld (%9.6f%%): '%c', %a, <%d bytes> (%t RAM)\n%!"
            idx (Int63.to_int64 off)
            (float_of_int idx /. float_of_int entry_count *. 100.)
            kind pp_key key len mem_usage;

        let reconstruction, new_errors =
          match reconstruction with
          | `None -> (`None, [])
          | `Some obj ->
              let obj_out =
                match obj with
                | (`Commit _ | `Blob _) as x -> x
                | `Node (x, _) -> `Node x
              in
              let errs = ref [] in
              let link_to_pred key' =
                if Hashtbl.mem pass2.Pass2.per_hash key' then
                  Hashgraph.add_edge graph key key'
                else
                  let e =
                    Fmt.str "Children hash %a not in pack file" pp_key key'
                  in
                  errs := e :: !errs
              in

              let deal_with_blob () = Hashgraph.add_vertex graph key in
              let deal_with_commit c =
                Hashgraph.add_vertex graph key;
                link_to_pred (Commit.node c);
                let date = Commit.info c |> Info.date in
                List.iter link_to_pred (Commit.parents c);
                commits_per_date := Datemap.add date key !commits_per_date
              in
              let deal_with_node (n, indirect_children) =
                Hashgraph.add_vertex graph key;
                let children =
                  Inode.Val.pred n
                  |> List.map (function `Contents h | `Inode h | `Node h -> h)
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
                        Fmt.str
                          "Possesses a children %a with higher offset %a (+%a)"
                          pp_key k (Repr.pp Int63.t) o (Repr.pp Int63.t)
                          Int63.(sub o off)
                      in
                      errs := e :: !errs)
                  indirect_children;
                List.iter
                  (fun k ->
                    let e =
                      Fmt.str
                        "Possesses a children by hash (direct children) %a"
                        pp_key k
                    in
                    errs := e :: !errs)
                  direct_children
              in
              (match obj with
              | `Blob _ -> deal_with_blob ()
              | `Commit c -> deal_with_commit c
              | `Node x -> deal_with_node x);
              (`Some obj_out, !errs)
        in
        let errors = errors @ new_errors in

        add_to_assoc_at_key per_hash key off
          { len; kind; reconstruction; errors };

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
        per_offset = pass2.Pass2.per_offset;
        extra_errors = pass2.Pass2.extra_errors;
      }
  end

  (** Find the commit connectivity *)
  module Pass4 = struct
    type value =
      [ `Blob of Contents.t | `Node of Inode.Val.t | `Commit of Commit_value.t ]

    type entry = {
      len : length;
      kind : kind;
      reconstruction :
        [ `None | `Some of value | `Bin_only of Inode_internal.Raw.t ];
      oldest_commit_successor : hash option;
      newest_commit_successor : hash option;
      errors : string list;
    }

    type t = {
      entry_count : int;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let run ~progress pass3 =
      let entry_count = pass3.Pass3.entry_count in
      let graph = pass3.Pass3.graph in

      let newest_commit_per_hash = Hashtbl.create entry_count in
      let rec visit ~commit_hash hash =
        if not @@ Hashtbl.mem newest_commit_per_hash hash then (
          Hashtbl.add newest_commit_per_hash hash commit_hash;
          if Hashtbl.length newest_commit_per_hash mod 3 = 0 then
            progress Int63.one;
          Hashgraph.iter_succ (visit ~commit_hash) graph hash)
      in
      let visit_commit (_date, hash) = visit ~commit_hash:hash hash in
      Datemap.to_rev_seq pass3.Pass3.commits_per_date |> Seq.iter visit_commit;

      let oldest_commit_per_hash = Hashtbl.create entry_count in
      let rec visit ~commit_hash hash =
        if not @@ Hashtbl.mem oldest_commit_per_hash hash then (
          Hashtbl.add oldest_commit_per_hash hash commit_hash;
          if Hashtbl.length oldest_commit_per_hash mod 3 = 0 then
            progress Int63.one;
          Hashgraph.iter_succ (visit ~commit_hash) graph hash)
      in
      let visit_commit (_date, hash) = visit ~commit_hash:hash hash in
      Datemap.to_seq pass3.Pass3.commits_per_date |> Seq.iter visit_commit;

      (* The [newest_commit_per_hash] table now contains all the hashes of all
         the objects reachable from a commit. The missing ones are orphan. *)
      let per_hash = Hashtbl.create entry_count in
      let aux off key idx =
        let Pass3.{ len; kind; reconstruction; errors } =
          Hashtbl.find pass3.Pass3.per_hash key |> List.assoc off
        in
        let newest_commit_successor =
          Hashtbl.find_opt newest_commit_per_hash key
        in
        let oldest_commit_successor =
          Hashtbl.find_opt oldest_commit_per_hash key
        in
        let errors =
          match newest_commit_successor with
          | Some _ -> errors
          | None -> "orphan from any commit" :: errors
        in
        add_to_assoc_at_key per_hash key off
          {
            len;
            kind;
            reconstruction;
            oldest_commit_successor;
            newest_commit_successor;
            errors;
          };
        if idx mod 3 = 0 then progress Int63.one;
        idx + 1
      in
      let (_ : int) = Offsetmap.fold aux pass3.Pass3.per_offset 0 in
      {
        entry_count = pass3.Pass3.entry_count;
        per_hash;
        per_offset = pass3.Pass3.per_offset;
        extra_errors = pass3.Pass3.extra_errors;
      }
  end

  (** Correlate with Index file *)
  module Pass5 = struct
    type value =
      [ `Blob of Contents.t | `Node of Inode.Val.t | `Commit of Commit_value.t ]
    [@@deriving irmin]

    type entry = {
      len : length;
      kind : kind;
      reconstruction :
        [ `None | `Some of value | `Bin_only of Inode_internal.Raw.t ];
      oldest_commit_successor : hash option;
      newest_commit_successor : hash option;
      errors : string list;
    }

    type t = {
      entry_count : int;
      byte_count : Int63.t;
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let run ~progress byte_count pass4 index =
      let entry_count = pass4.Pass4.entry_count in
      let remaining_index_keys = Hashtbl.create entry_count in
      let remaining_index_offs = Hashtbl.create entry_count in
      let invalid_index_keys = Hashtbl.create 1000 in
      let invalid_index_offs = Hashtbl.create 1000 in

      (* Step 1 - Detect index entries not mapping well to pack *)
      let aux key (off, len, kind) =
        let kind = Pack_value.Kind.to_magic kind in
        match Offsetmap.locate pass4.Pass4.per_offset off with
        | (`Below _ | `Above _ | `Between _ | `Empty) as loc ->
            let e =
              Fmt.str
                "The index entry hash=%a, off=%a, len=%d, kind=%c maps to the \
                 pack file to %a"
                pp_key key (Repr.pp Int63.t) off len kind
                (Repr.pp (Offsetmap.location_t Hash.t))
                loc
            in
            Hashtbl.add invalid_index_keys key e;
            Hashtbl.add invalid_index_offs off e
        | `Exact (_, key') ->
            let Pass4.{ len = len'; kind = kind'; _ } =
              Hashtbl.find pass4.Pass4.per_hash key |> List.assoc off
            in
            if not (key_equal key key' && len = len' && kind = kind') then (
              let e =
                Fmt.str
                  "The index entry off=%a, hash=%a, len=%d, kind=%c maps to \
                   the pack file to hash=%a, len=%d, kind=%c"
                  (Repr.pp Int63.t) off pp_key key len kind pp_key key' len'
                  kind'
              in
              Hashtbl.add invalid_index_keys key e;
              Hashtbl.add invalid_index_keys key' e;
              Hashtbl.add invalid_index_offs off e)
            else (
              Hashtbl.add remaining_index_offs off key;
              Hashtbl.add remaining_index_keys key off)
      in
      Index.iter aux index;
      progress Int63.one;
      Printf.eprintf
        "\n remaining_index_keys = %#d, invalid_index_keys = %#d\n%!"
        (Hashtbl.length remaining_index_keys)
        (Hashtbl.length invalid_index_keys);

      (* Step 2 - Detect duplicate index entries *)
      let aux key off =
        let offs = Hashtbl.find_all remaining_index_keys key in
        let keys = Hashtbl.find_all remaining_index_offs off in
        match (keys, offs) with
        | [ _ ], [ _ ] -> ()
        | [], _ | _, [] -> assert false
        | _, _ ->
            let e =
              Fmt.str
                "Index's off=%a maps to key(s)=[%a]. Index's hash=%a maps to \
                 off(s)=[%a]."
                (Repr.pp Int63.t) off
                Fmt.(list ~sep:semi pp_key)
                keys pp_key key
                Fmt.(list ~sep:semi (Repr.pp Int63.t))
                offs
            in
            Hashtbl.add invalid_index_keys key e;
            Hashtbl.add invalid_index_offs off e
      in
      Hashtbl.iter aux remaining_index_keys;
      Hashtbl.to_seq_keys invalid_index_keys
      |> Seq.iter (Hashtbl.remove remaining_index_keys);
      Hashtbl.to_seq_keys invalid_index_offs
      |> Seq.iter (Hashtbl.remove remaining_index_offs);
      progress Int63.one;
      Printf.eprintf
        "\n remaining_index_keys = %#d, invalid_index_keys = %#d\n%!"
        (Hashtbl.length remaining_index_keys)
        (Hashtbl.length invalid_index_keys);

      (* Step 3 - Detect entries that fail to be [find] *)
      let aux key off =
        match Index.find index key with
        | None ->
            let e =
              Fmt.str "Index's find missed %a / %a" pp_key key (Repr.pp Int63.t)
                off
            in
            Hashtbl.add invalid_index_keys key e;
            Hashtbl.add invalid_index_offs off e
        | Some (off', _, _) ->
            if not (Int63.equal off off') then (
              let e =
                Fmt.str "Index's find gave %a for %a instead of %a "
                  (Repr.pp Int63.t) off' pp_key key (Repr.pp Int63.t) off
              in
              Hashtbl.add invalid_index_keys key e;
              Hashtbl.add invalid_index_offs off e)
      in
      Hashtbl.iter aux remaining_index_keys;
      Hashtbl.to_seq_keys invalid_index_keys
      |> Seq.iter (Hashtbl.remove remaining_index_keys);
      Hashtbl.to_seq_keys invalid_index_offs
      |> Seq.iter (Hashtbl.remove remaining_index_offs);
      progress Int63.one;
      Printf.eprintf
        "\n remaining_index_keys = %#d, invalid_index_keys = %#d\n%!"
        (Hashtbl.length remaining_index_keys)
        (Hashtbl.length invalid_index_keys);

      (* Step 4 - Attach the Index errors to their pack entry *)
      let per_hash = Hashtbl.create entry_count in
      let aux off key idx =
        let Pass4.
              {
                len;
                kind;
                reconstruction;
                oldest_commit_successor;
                newest_commit_successor;
                errors;
              } =
          Hashtbl.find pass4.Pass4.per_hash key |> List.assoc off
        in
        let new_errors =
          Hashtbl.find_all invalid_index_keys key
          @ Hashtbl.find_all invalid_index_offs off
          |> List.sort_uniq Stdlib.compare
        in
        let new_errors =
          match new_errors with
          | [] ->
              if Hashtbl.mem remaining_index_keys key then []
              else "No trace of this hash/offset in index" :: new_errors
          | l -> l
        in
        let errors = errors @ new_errors in
        add_to_assoc_at_key per_hash key off
          {
            len;
            kind;
            reconstruction;
            oldest_commit_successor;
            newest_commit_successor;
            errors;
          };
        idx + 1
      in
      let (_ : int) = Offsetmap.fold aux pass4.Pass4.per_offset 0 in
      progress Int63.one;

      (* Step 5 - Remember the orphan index errors *)
      let rec remove_loop table k =
        let len = Hashtbl.length table in
        Hashtbl.remove table k;
        let len' = Hashtbl.length table in
        if len <> len' then remove_loop table k
      in
      Offsetmap.iter
        (fun off key ->
          remove_loop invalid_index_keys key;
          remove_loop invalid_index_offs off)
        pass4.Pass4.per_offset;
      let new_extra_errors =
        (Hashtbl.to_seq_values invalid_index_keys |> List.of_seq)
        @ (Hashtbl.to_seq_values invalid_index_offs |> List.of_seq)
        |> List.sort_uniq Stdlib.compare
      in
      Printf.eprintf
        "\n remaining_index_keys = %#d, invalid_index_keys = %#d\n%!"
        (Hashtbl.length remaining_index_keys)
        (Hashtbl.length invalid_index_keys);
      progress Int63.one;

      {
        byte_count;
        entry_count = pass4.Pass4.entry_count;
        per_hash;
        per_offset = pass4.Pass4.per_offset;
        extra_errors = pass4.Pass4.extra_errors @ new_extra_errors;
      }

    let is_entry_errorless = function { errors = []; _ } -> true | _ -> false
    let is_entry_commit = function { kind = 'C'; _ } -> true | _ -> false

    let pp_commit_successor ppf (per_hash, x) =
      match x with
      | None -> Format.fprintf ppf "none"
      | Some key -> (
          match Hashtbl.find per_hash key with
          | [ (off, { reconstruction = `Some (`Commit v) }) ] ->
              Format.fprintf ppf "hash:%a, off:%#Ld, %a" pp_key key
                (Int63.to_int64 off) (Repr.pp Commit_value.t) v
          | _ -> Format.fprintf ppf "<could not retrive commit reconstruction>")

    let pp_reconstruction ppf = function
      | `None -> Format.fprintf ppf "none"
      | `Some v -> Format.fprintf ppf "%a" (Repr.pp value_t) v
      | `Bin_only v ->
          Format.fprintf ppf "(Inode.Bin) %a" (Repr.pp Inode_internal.Raw.t) v

    let pp_entry ppf (per_hash, idx, entry_count, off, byte_count, key, entry) =
      Format.fprintf ppf
        "%#12dth entry (total %#12d) at offset %#13Ld (total %#13Ld, %9.6f%%) \
         '%c', %a, <%d bytes>\n\
         oldest_commit_successor: %a\n\
         newest_commit_successor: %a\n\
         reconstruction: %a%a" idx entry_count (Int63.to_int64 off)
        (Int63.to_int64 byte_count)
        (Int63.to_float off /. Int63.to_float byte_count *. 100.)
        entry.kind pp_key key entry.len pp_commit_successor
        (per_hash, entry.oldest_commit_successor)
        pp_commit_successor
        (per_hash, entry.newest_commit_successor)
        pp_reconstruction entry.reconstruction
        Fmt.(list string)
        (List.map (Printf.sprintf "\nERROR: %s") entry.errors)

    let pp_errorless_commit ppf (idx, entry_count, off, byte_count, key, entry)
        =
      Format.fprintf ppf
        "%#12dth entry (total %#12d) at offset %#13Ld (total %#13Ld, %9.6f%%) \
         '%c', %a, <%d bytes>\n\
         reconstruction: %a" idx entry_count (Int63.to_int64 off)
        (Int63.to_int64 byte_count)
        (Int63.to_float off /. Int63.to_float byte_count *. 100.)
        entry.kind pp_key key entry.len pp_reconstruction entry.reconstruction

    let spacer =
      "****************************************************************************************************"

    let pp ppf
        { per_offset; per_hash; entry_count; byte_count; extra_errors; _ } =
      let idx = ref 0 in
      Offsetmap.iter
        (fun off key ->
          let entry = Hashtbl.find per_hash key |> List.assoc off in
          let has_errors = not @@ is_entry_errorless entry in
          let is_commit = is_entry_commit entry in
          if has_errors then
            Format.fprintf ppf "%s\n%a\n%s\n\n%!" spacer pp_entry
              (per_hash, !idx, entry_count, off, byte_count, key, entry)
              spacer
          else if is_commit then
            Format.fprintf ppf "%s\n%a\n%s\n\n%!" spacer pp_errorless_commit
              (!idx, entry_count, off, byte_count, key, entry)
              spacer;
          incr idx)
        per_offset;
      List.iter
        (fun err -> Format.fprintf ppf "extra error: %s\n" err)
        extra_errors
  end

  let with_bar ?(sampling_interval = 10_000) ~total ~phase f =
    let message = Printf.sprintf "Brute force integrity check, pass%d" phase in
    let bar, progress =
      Utils.Progress.counter ~total ~sampling_interval ~message ()
    in
    let res = f ~progress in
    Utils.Progress.finalise bar;
    res

  let run config =
    if Conf.readonly config then raise S.RO_not_allowed;
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
      let pass0 =
        with_bar ~phase:0 ~total:byte_count (Pass0.run ~byte_count pack)
      in
      Printf.eprintf "Pass0 - done\n%!";
      pass0
    in

    let pass1 () =
      let pass0 = pass0 () in
      (* Gc.full_major (); *)
      let entry_count = Int63.of_int pass0.Pass0.entry_count in
      Printf.eprintf "Pass1 - go\n%!";
      let pass1 =
        with_bar ~phase:1 ~total:entry_count
          (Pass1.run ~byte_count pack dict pass0)
      in
      Printf.eprintf "Pass1 - done\n%!";
      pass1
    in

    let pass2 () =
      let pass1 = pass1 () in
      (* Gc.full_major (); *)
      let entry_count = Int63.of_int pass1.Pass1.entry_count in
      Printf.eprintf "Pass2 - go\n%!";
      let pass2 = with_bar ~phase:2 ~total:entry_count (Pass2.run pass1) in
      Printf.eprintf "Pass2 - done\n%!";
      pass2
    in

    let pass3 () =
      let pass2 = pass2 () in
      (* Gc.full_major (); *)
      Printf.eprintf "Pass3 - go\n%!";
      let entry_count = Int63.of_int pass2.Pass2.entry_count in
      let pass3 = with_bar ~phase:3 ~total:entry_count (Pass3.run pass2) in
      Printf.eprintf "Pass3 - done\n%!";
      pass3
    in

    let pass4 () =
      let pass3 = pass3 () in
      (* Gc.full_major (); *)
      Printf.eprintf "Pass4 - go\n%!";
      let entry_count = Int63.of_int pass3.Pass3.entry_count in
      let pass4 = with_bar ~phase:4 ~total:entry_count (Pass4.run pass3) in
      Printf.eprintf "Pass4 - done\n%!";
      pass4
    in

    let pass5 () =
      let pass4 = pass4 () in
      (* Gc.full_major (); *)
      Printf.eprintf "Pass5 - go\n%!";
      let pass5 =
        with_bar ~phase:5 ~total:(Int63.of_int 5)
          (Pass5.run byte_count pass4 index)
          ~sampling_interval:1
      in
      Printf.eprintf "Pass5 - done\n%!";
      pass5
    in

    let pass5 = pass5 () in
    ignore index;
    IO.close pack;
    fun ppf ->
      Format.fprintf ppf "%a\n%!" Pass5.pp pass5;
      ()
end
