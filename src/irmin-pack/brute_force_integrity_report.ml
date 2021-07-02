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
  module Commit : Pack_value.S with type hash = Hash.t
end

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

  type offset = O of int63 [@@unboxed]
  let offset_t = Repr.(map int63_t (fun i -> O i) (fun (O i) -> i))

  module Offset = struct
    type t = offset [@@deriving irmin]

  end

  type hash = Hash.t [@@deriving irmin]
  type length = L of int [@@unboxed]
  type kind = char [@@deriving irmin]
  type ('k, 'v) assoc = ('k * 'v) list [@@deriving irmin]

  module Index = struct
    include Index

    let value_t : value Irmin.Type.t =
      Irmin.Type.(triple int63_t int Pack_value.Kind.t)
  end

  module Offsetmap = struct
    include Stdlib.Map.Make (struct
      type t = offset

      let compare (O v) (O v') = Int63.compare v v'
    end)

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

  module Offsetgraph = Graph.Imperative.Digraph.Concrete (struct
    type t = offset

    let compare (O v) (O v') = Int63.compare v v'
    let equal = ( = )
    let hash (O v) = Repr.(short_hash Int63.t |> unstage) v
  end)

  (** Read pack file *)
  module Pass0 = struct
    type entry = { length : length; kind : kind; bytes : string }

    type content = {
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }

    let decode_entry_length = function
      | Pack_value.Kind.Contents -> Contents.decode_bin_length
      | Commit -> Commit.decode_bin_length
      | Node | Inode -> Inode.decode_bin_length

    let decode_entry_exn ~off ~buffer ~buffer_off =
      try
        (* Decode the key and kind by hand *)
        let off_after_key, key = decode_key buffer buffer_off in
        assert (off_after_key = buffer_off + Hash.hash_size);
        let off_after_kind, kind = decode_kind buffer off_after_key in
        assert (off_after_kind = buffer_off + Hash.hash_size + 1);
        (* Get the length of the entire entry *)
        let len = decode_entry_length kind buffer buffer_off in
        (off, len, kind, key)
      with
      | Invalid_argument msg when msg = "index out of bounds" ->
          raise Not_enough_buffer
      | Invalid_argument msg when msg = "String.blit / Bytes.blit_string" ->
          raise Not_enough_buffer

    let fold_entries ~total pack f =
      let buffer = ref (Bytes.create 1024) in
      let refill_buffer ~from =
        let read = IO.read pack ~off:from !buffer in
        (* @craigfe, the following should be upstreamed *)
        let filled = read = Bytes.length !buffer in
        let eof = Int63.equal total (Int63.add from (Int63.of_int read)) in
        if (not filled) && not eof then
          Fmt.failwith
            "When refilling from offset %#Ld (total %#Ld), read %#d but \
             expected %#d"
            (Int63.to_int64 from) (Int63.to_int64 total) read
            (Bytes.length !buffer)
      in
      let expand_and_refill_buffer ~from =
        let length = Bytes.length !buffer in
        if length > 1_000_000_000 (* 1 GB *) then
          Fmt.failwith
            "Couldn't decode the value at offset %a in %d of buffer space. \
             Corrupted data file?"
            Int63.pp from length
        else (
          buffer := Bytes.create (2 * length);
          refill_buffer ~from)
      in
      let rec aux ~buffer_off off acc =
        assert (off <= total);
        if off = total then acc
        else
          let buffer_off, off, acc =
            match
              decode_entry_exn ~off
                ~buffer:(Bytes.unsafe_to_string !buffer)
                ~buffer_off
            with
            | entry ->
                let off', entry_len, kind, _ = entry in
                let entry_lenL = Int63.of_int entry_len in
                (buffer_off + entry_len, off ++ entry_lenL, f acc entry)
            | exception Not_enough_buffer ->
                let () =
                  if buffer_off > 0 then
                    (* Try again with the value at the start of the buffer. *)
                    refill_buffer ~from:off
                  else
                    (* The entire buffer isn't enough to hold this value: expand it. *)
                    expand_and_refill_buffer ~from:off
                in
                (0, off, acc)
          in
          aux ~buffer_off off acc
      in
      ()

    let run ~progress ~total pack =

      let f (idx, per_offset) (off, len, kind, key) =
        progress len;
        if idx mod 5_000_000 = 0 then
          Fmt.epr "\n%#12dth at %#Ld (%.6f%%): '%a', %a, <%d bytes>\n%!" idx
            (Int63.to_int64 off)
            (Int63.to_float off /. Int63.to_float total *. 100.)
            Pack_value.Kind.pp kind pp_key key len;
        let per_offset = Offsetmap.add off key per_offset in
        (idx + 1, per_offset)
      in
      ()
          (* progress entry_lenL; *)

      (* { per_offset; per_hash; extra_errors } *)
  end

  (** Rebuild values (and check their integrity) *)
  module Pass1 = struct
    type value =
      [ `Contents of Contents.t | `Node of Inode.Val.t | `Commit of Commit.t ]

    type entry = {
      length : length;
      kind : kind;
      reconstruction : (string, value) result;
    }

    type content = {
      per_offset : hash Offsetmap.t;
      per_hash : (hash, (offset, entry) assoc) Hashtbl.t;
      extra_errors : string list;
    }
  end

  (** Produce higher level insights *)
  module Pass2 = struct
    type t
    (* type reconstruction = {
     *   preds :
     *     [ `Contents of offset * [ `Offset | `Hash ]
     *     | `Node of offset * [ `Offset | `Hash ]
     *     | `Commit of offset ]
     *     list;
     * } *)
    (* graph : Offsetgraph.t; *)
  end

  let ingest_data_file ~progress ~total pack dict old_index () =
    (* let io_read_and_decode_hash ~off =
     *   let buf = Bytes.create Hash.hash_size in
     *   let n = IO.read pack ~off buf in
     *   assert (n = Hash.hash_size);
     *   let _, v = decode_key (Bytes.unsafe_to_string buf) 0 in
     *   v
     * in
     * let call_decode_bin f ~off ~len =
     *   let buf = Bytes.create len in
     *   let n = IO.read pack ~off buf in
     *   if n <> len then failwith "Didn't read enough";
     *   let hash off = io_read_and_decode_hash ~off in
     *   let dict = Dict.find dict in
     *   f ~dict ~hash (Bytes.unsafe_to_string buf) 0
     * in *)

    (* let rebuild_blob ~len ~off =
     *   call_decode_bin Contents.decode_bin ~off ~len |> snd
     * in
     * let rebuild_commit ~len ~off =
     *   call_decode_bin Commit.decode_bin ~off ~len |> snd
     * in
     * let rebuild_inode_raw ~len ~off =
     *   call_decode_bin Inode_internal.Raw.decode_bin ~off ~len |> snd
     * in
     * let rebuild_inode =
     *   Inode_internal.Val.of_raw (fun h ->
     *       match Index.find old_index h with
     *       | None -> None
     *       | Some (off, len, kind) -> (
     *           match Pack_value.Kind.to_magic kind with
     *           | 'I' | 'N' -> Some (rebuild_inode_raw ~len ~off)
     *           | _ -> assert false))
     * in *)
    let thresh = Int63.to_float total *. 0.988 in

    let f ({ idx; pack_segmentation } as acc)
        ({ key; data = off, len, kind } as entry) =
      let pack_segmentation = Offsetmap.add off entry pack_segmentation in
      if idx mod 5_000_000 = 0 then
        Fmt.epr "\n%#12dth at %#Ld (%.6f%%): '%a', %a, <%d bytes>\n%!" idx
          (Int63.to_int64 off)
          (Int63.to_float off /. Int63.to_float total *. 100.)
          Pack_value.Kind.pp kind pp_key key len;

      (* if true then failwith "super"; *)
      { acc with idx = idx + 1; pack_segmentation }
      (* let off', entry_len, kind = data in
       * let entry_lenL = Int63.of_int entry_len in
       * assert (off = off');
       *
       * if not @@ Index.mem old_index key then
       *   Fmt.epr "\nk = %a (off, len, kind) = (%a (%.6f%%), %d, %a).\n%!" pp_key
       *     key Int63.pp off
       *     (Int63.to_float off /. Int63.to_float total *. 100.)
       *     entry_len Pack_value.Kind.pp kind;
       *
       * (if Int63.to_float off >= thresh then
       *    match Pack_value.Kind.to_magic kind with
       *    | 'C' ->
       *       let _ = rebuild_commit ~off:off' ~len:entry_len in
       *       ()
       *    | 'B' ->
       *       let _ = rebuild_blob ~off:off' ~len:entry_len in
       *       ()
       *    | 'N' | 'I' -> (
       *      let raw = rebuild_inode_raw ~off:off' ~len:entry_len in
       *      try raw |> rebuild_inode |> ignore
       *      with e ->
       *        Fmt.epr
       *          "\n\
       *           k = %a (off, len, kind) = (%a (%.6f%%), %d, %a).\n\
       *           \  error:%s\n\
       *           \  truc: %a\n\
       *           %!"
       *          pp_key key Int63.pp off
       *          (Int63.to_float off /. Int63.to_float total *. 100.)
       *          entry_len Pack_value.Kind.pp kind (Printexc.to_string e)
       *          (Repr.pp Inode_internal.Raw.t)
       *          raw)
       *    | _ -> assert false);
       *
       * () *)
    in
    refill_buffer ~from:Int63.zero;
    fold_entries ~buffer_off:0 Int63.zero f
      { idx = 0; pack_segmentation = Offsetmap.empty }

  let run config =
    if Conf.readonly config then raise S.RO_not_allowed;
    let run_duration = Mtime_clock.counter () in
    let root = Conf.root config in
    let log_size = Conf.index_log_size config in
    Log.app (fun f ->
        f "Beginning index reconstruction with parameters: { log_size = %d }"
          log_size);
    (* let index = Index.v ~fresh:true ~readonly:false ~log_size dest in *)
    let old_index = Index.v ~fresh:false ~readonly:true ~log_size root in
    let pack_file = Filename.concat root "store.pack" in
    let pack =
      IO.v ~fresh:false ~readonly:true ~version:(Some Version.version) pack_file
    in
    let dict = Dict.v ~fresh:false ~readonly:true root in

    let total = IO.offset pack in
    let bar, progress =
      Utils.Progress.counter ~total ~sampling_interval:100
        ~message:"Reconstructing index" ~pp_count:Utils.pp_bytes ()
    in
    let { pack_segmentation; _ } =
      ingest_data_file ~progress ~total pack dict old_index ()
    in

    ignore pack_segmentation;
    Utils.Progress.finalise bar;

    Printf.printf "hello\n%!";
    Fmt.pr "test: %a\n%!" (Irmin.Type.pp location_t)
      (Offsetmap.locate pack_segmentation (Int63.of_int (-1)));
    Fmt.pr "test: %a\n%!" (Irmin.Type.pp location_t)
      (Offsetmap.locate pack_segmentation (Int63.of_int 0));
    Fmt.pr "test: %a\n%!" (Irmin.Type.pp location_t)
      (Offsetmap.locate pack_segmentation (Int63.of_int 1));
    Fmt.pr "test: %a\n%!" (Irmin.Type.pp location_t)
      (Offsetmap.locate pack_segmentation (Int63.of_int 10_000));
    Fmt.pr "test: %a\n%!" (Irmin.Type.pp location_t)
      (Offsetmap.locate pack_segmentation (Int63.of_int 10000000_000));
    Printf.printf "hello\n%!";

    (* Ensure that the log file is empty, so that subsequent opens with a
       smaller [log_size] don't immediately trigger a merge operation. *)
    Log.app (fun f ->
        f "Completed indexing of pack entries. Running a final merge ...");
    (* Index.try_merge index; *)
    (* Index.close index; *)
    IO.close pack;
    fun ppf -> Format.fprintf ppf "success!!"
end
