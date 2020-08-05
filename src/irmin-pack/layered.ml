(*
 * Copyright (c) 2013-2020 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESIrmin. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

let src = Logs.Src.create "irmin.layered" ~doc:"Irmin layered store"

module Log = (val Logs.src_log src : Logs.LOG)

open Lwt.Infix

module type CA = sig
  include Pack.S

  module Key : Irmin.Hash.TYPED with type t = key and type value = value
end

module Copy
    (Key : Irmin.Hash.S)
    (SRC : Pack.S with type key = Key.t)
    (DST : Pack.S with type key = SRC.key and type value = SRC.value) =
struct
  let add_to_dst add (k, v) = add k v

  let stats = function
    | "Contents" -> Irmin_layers.Stats.copy_contents ()
    | "Node" -> Irmin_layers.Stats.copy_nodes ()
    | "Commit" -> Irmin_layers.Stats.copy_commits ()
    | _ -> failwith "unexpected type in stats"

  let already_in_dst ~dst k =
    DST.mem dst k >|= function
    | true ->
        Log.debug (fun l -> l "already in dst %a" (Irmin.Type.pp Key.t) k);
        true
    | false -> false

  let copy ~src ~dst ?(aux = fun _ -> Lwt.return_unit) str k =
    Log.debug (fun l -> l "copy %s %a" str (Irmin.Type.pp Key.t) k);
    stats str;
    SRC.find src k >>= function
    | None -> Lwt.return_unit
    | Some v -> aux v >>= fun () -> add_to_dst (DST.unsafe_add dst) (k, v)

  let check_and_copy ~src ~dst ?aux str k =
    already_in_dst ~dst k >>= function
    | true -> Lwt.return_unit
    | false -> copy ~src ~dst ?aux str k
end

module Content_addressable
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (Pack : Pack.S with type index = Index.t and type key = H.t) :
  S.LAYERED_CONTENT_ADDRESSABLE_STORE
    with type key = Pack.key
     and type value = Pack.value
     and type index = Pack.index
     and module U = Pack
     and module L = Pack = struct
  type index = Pack.index

  type key = Pack.key

  type value = Pack.value

  module U = Pack
  module L = Pack

  type 'a t = {
    lower : [ `Read ] L.t option;
    mutable flip : bool;
    uppers : [ `Read ] U.t * [ `Read ] U.t;
  }

  let v upper1 upper0 lower ~flip = { lower; flip; uppers = (upper1, upper0) }

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  let lower t = Option.get t.lower

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  let mem_lower t k = Option.get t.lower |> fun lower -> L.mem lower k

  let mem_current t k = U.mem (current_upper t) k

  let add t v =
    Log.debug (fun l -> l "add in %s" (log_current_upper t));
    Irmin_layers.Stats.add ();
    let upper = current_upper t in
    U.add upper v

  let unsafe_add t k v =
    Log.debug (fun l -> l "unsafe_add in %s" (log_current_upper t));
    Irmin_layers.Stats.add ();
    let upper = current_upper t in
    U.unsafe_add upper k v

  let unsafe_append t k v =
    Log.debug (fun l -> l "unsafe_append in %s" (log_current_upper t));
    let upper = current_upper t in
    U.unsafe_append upper k v

  (* RO instances do not know which of the two uppers is in use by RW, so find
     (and mem) has to look in both uppers. *)
  let find t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None -> (
            match t.lower with
            | None -> Lwt.return_none
            | Some lower ->
                Log.debug (fun l -> l "find in lower");
                L.find lower k))

  let unsafe_find t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "unsafe_find in %s" (log_current_upper t));
    match U.unsafe_find current k with
    | Some v -> Some v
    | None -> (
        Log.debug (fun l -> l "unsafe_find in %s" (log_previous_upper t));
        match U.unsafe_find previous k with
        | Some v -> Some v
        | None -> (
            match t.lower with
            | None -> None
            | Some lower ->
                Log.debug (fun l -> l "unsafe_find in lower");
                L.unsafe_find lower k))

  let mem t k =
    let current = current_upper t in
    let previous = previous_upper t in
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false -> (
            match t.lower with
            | None -> Lwt.return_false
            | Some lower -> L.mem lower k))

  let unsafe_mem t k =
    let current = current_upper t in
    let previous = previous_upper t in
    U.unsafe_mem current k
    || U.unsafe_mem previous k
    || match t.lower with None -> false | Some lower -> L.unsafe_mem lower k

  let flush ?index t =
    U.flush ?index (fst t.uppers);
    U.flush ?index (snd t.uppers);
    match t.lower with None -> () | Some x -> L.flush ?index x

  let cast t = (t :> [ `Read | `Write ] t)

  let batch t f =
    f (cast t) >|= fun r ->
    flush ~index:true t;
    r

  let sync ?on_generation_change t =
    U.sync ?on_generation_change (fst t.uppers);
    U.sync ?on_generation_change (snd t.uppers);
    match t.lower with None -> () | Some x -> L.sync ?on_generation_change x

  let close t =
    U.close (fst t.uppers) >>= fun () ->
    U.close (snd t.uppers) >>= fun () ->
    match t.lower with None -> Lwt.return_unit | Some x -> L.close x

  type integrity_error = U.integrity_error

  let integrity_check ~offset ~length k t =
    let current = current_upper t in
    let previous = previous_upper t in
    let lower = t.lower in
    match
      ( U.integrity_check ~offset ~length k current,
        U.integrity_check ~offset ~length k previous,
        match lower with
        | None -> Ok ()
        | Some lower -> L.integrity_check ~offset ~length k lower )
    with
    | Ok (), Ok (), Ok () -> Ok ()
    | Error `Wrong_hash, _, _
    | _, Error `Wrong_hash, _
    | _, _, Error `Wrong_hash ->
        Error `Wrong_hash
    | Error `Absent_value, _, _
    | _, Error `Absent_value, _
    | _, _, Error `Absent_value ->
        Error `Absent_value

  let layer_id t k =
    U.mem (fst t.uppers) k >>= function
    | true -> Lwt.return `Upper1
    | false -> (
        U.mem (snd t.uppers) k >>= function
        | true -> Lwt.return `Upper0
        | false -> (
            match t.lower with
            | None -> raise Not_found
            | Some lower -> (
                L.mem lower k >|= function
                | true -> `Lower
                | false -> raise Not_found)))

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () ->
    match t.lower with None -> Lwt.return_unit | Some x -> L.clear x

  let clear_caches t =
    U.clear_caches (fst t.uppers);
    U.clear_caches (snd t.uppers);
    match t.lower with None -> () | Some x -> L.clear_caches x

  (** After clearing the previous upper, we also needs to flush current upper to
      disk, otherwise values are not found by the RO. *)
  let clear_previous_upper t =
    let previous = previous_upper t in
    let current = current_upper t in
    U.flush current;
    U.clear previous

  let version t = U.version (fst t.uppers)

  let flip_upper t =
    Log.debug (fun l -> l "flip_upper to %s" (log_previous_upper t));
    t.flip <- not t.flip

  module CopyUpper = Copy (H) (U) (U)
  module CopyLower = Copy (H) (U) (L)

  type 'a layer_type =
    | Upper : [ `Read ] U.t layer_type
    | Lower : [ `Read ] L.t layer_type

  let check_and_copy_to_lower t ~dst ?aux str k =
    CopyLower.check_and_copy ~src:(previous_upper t) ~dst ?aux str k

  let check_and_copy_to_current t ~dst ?aux str (k : key) =
    CopyUpper.check_and_copy ~src:(previous_upper t) ~dst ?aux str k

  let copy_to_lower t ~dst ?aux str k =
    CopyLower.copy ~src:(previous_upper t) ~dst ?aux str k

  let copy_to_current t ~dst ?aux str (k : key) =
    CopyUpper.copy ~src:(previous_upper t) ~dst ?aux str k

  let check_and_copy :
      type l.
      l layer_type * l ->
      [ `Read ] t ->
      ?aux:(value -> unit Lwt.t) ->
      string ->
      key ->
      unit Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> check_and_copy_to_lower ~dst
    | Upper -> check_and_copy_to_current ~dst

  let copy :
      type l.
      l layer_type * l ->
      [ `Read ] t ->
      ?aux:(value -> unit Lwt.t) ->
      string ->
      key ->
      unit Lwt.t =
   fun (ltype, dst) ->
    match ltype with
    | Lower -> copy_to_lower ~dst
    | Upper -> copy_to_current ~dst
end

module Pack_Maker
    (H : Irmin.Hash.S)
    (Index : Pack_index.S)
    (P : Pack.MAKER with type key = H.t and type index = Index.t) :
  S.LAYERED_MAKER with type key = P.key and type index = P.index = struct
  type index = P.index

  type key = P.key

  module Make (V : Pack.ELT with type hash := key) = struct
    module Upper = P.Make (V)
    include Content_addressable (H) (Index) (Upper)
  end
end

module Atomic_write
    (K : Irmin.Branch.S)
    (A : S.ATOMIC_WRITE_STORE with type key = K.t) :
  S.LAYERED_ATOMIC_WRITE_STORE with type key = A.key and type value = A.value =
struct
  type key = A.key

  type value = A.value

  module U = A
  module L = A

  type t = { lower : L.t option; mutable flip : bool; uppers : U.t * U.t }

  let current_upper t = if t.flip then fst t.uppers else snd t.uppers

  let previous_upper t = if t.flip then snd t.uppers else fst t.uppers

  let log_current_upper t = if t.flip then "upper1" else "upper0"

  let log_previous_upper t = if t.flip then "upper0" else "upper1"

  (* RO instances do not know which of the two uppers is in use by RW, so find
     (and mem) has to look in both uppers. TODO if branch exists in both
     uppers, then we have to check which upper is in use by RW. *)
  let mem t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] mem in %s" (log_current_upper t));
    U.mem current k >>= function
    | true -> Lwt.return_true
    | false -> (
        Log.debug (fun l -> l "[branches] mem in%s" (log_previous_upper t));
        U.mem previous k >>= function
        | true -> Lwt.return_true
        | false -> (
            match t.lower with
            | None -> Lwt.return_false
            | Some lower ->
                Log.debug (fun l -> l "[branches] mem in lower");
                L.mem lower k))

  let find t k =
    let current = current_upper t in
    let previous = previous_upper t in
    Log.debug (fun l -> l "[branches] find in %s" (log_current_upper t));
    U.find current k >>= function
    | Some v -> Lwt.return_some v
    | None -> (
        Log.debug (fun l -> l "[branches] find in %s" (log_previous_upper t));
        U.find previous k >>= function
        | Some v -> Lwt.return_some v
        | None -> (
            match t.lower with
            | None -> Lwt.return_none
            | Some lower ->
                Log.debug (fun l -> l "[branches] find in lower");
                L.find lower k))

  let set t k v =
    Log.debug (fun l ->
        l "set %a in %s" (Irmin.Type.pp K.t) k (log_current_upper t));
    let upper = current_upper t in
    U.set upper k v

  (** Copy back into upper the branch against we want to do test and set. *)
  let test_and_set t k ~test ~set =
    let current = current_upper t in
    let previous = previous_upper t in
    let find_in_lower () =
      (match t.lower with
      | None -> Lwt.return_none
      | Some lower -> L.find lower k)
      >>= function
      | None -> U.test_and_set current k ~test:None ~set
      | Some v ->
          U.set current k v >>= fun () -> U.test_and_set current k ~test ~set
    in
    U.mem current k >>= function
    | true -> U.test_and_set current k ~test ~set
    | false -> (
        U.find previous k >>= function
        | None -> find_in_lower ()
        | Some v ->
            U.set current k v >>= fun () -> U.test_and_set current k ~test ~set)

  let remove t k =
    U.remove (fst t.uppers) k >>= fun () ->
    U.remove (snd t.uppers) k >>= fun () ->
    match t.lower with
    | None -> Lwt.return_unit
    | Some lower -> L.remove lower k

  let list t =
    U.list (fst t.uppers) >>= fun upper1 ->
    U.list (snd t.uppers) >>= fun upper2 ->
    (match t.lower with None -> Lwt.return_nil | Some lower -> L.list lower)
    >|= fun lower ->
    List.fold_left
      (fun acc b -> if List.mem b acc then acc else b :: acc)
      lower (upper1 @ upper2)

  type watch = U.watch

  let watch t = U.watch (current_upper t)

  let watch_key t = U.watch_key (current_upper t)

  let unwatch t = U.unwatch (current_upper t)

  let close t =
    U.close (fst t.uppers) >>= fun () ->
    U.close (snd t.uppers) >>= fun () ->
    match t.lower with None -> Lwt.return_unit | Some x -> L.close x

  let v upper1 upper0 lower ~flip = { lower; flip; uppers = (upper1, upper0) }

  let clear t =
    U.clear (fst t.uppers) >>= fun () ->
    U.clear (snd t.uppers) >>= fun () ->
    match t.lower with None -> Lwt.return_unit | Some x -> L.clear x

  let flush t =
    U.flush (fst t.uppers);
    U.flush (snd t.uppers);
    match t.lower with None -> () | Some x -> L.flush x

  (** Do not copy branches that point to commits not copied. *)
  let copy ~mem_commit_lower ~mem_commit_upper t =
    let previous = previous_upper t in
    let current = current_upper t in
    U.list previous >>= fun branches ->
    Lwt_list.iter_p
      (fun branch ->
        U.find previous branch >>= function
        | None -> Lwt.fail_with "branch not found in previous upper"
        | Some hash -> (
            (match t.lower with
            | None -> Lwt.return_unit
            | Some lower -> (
                mem_commit_lower hash >>= function
                | true ->
                    Log.debug (fun l ->
                        l "[branches] copy to lower %a" (Irmin.Type.pp K.t)
                          branch);
                    Irmin_layers.Stats.copy_branches ();
                    L.set lower branch hash
                | false -> Lwt.return_unit))
            >>= fun () ->
            mem_commit_upper hash >>= function
            | true ->
                Log.debug (fun l ->
                    l "[branches] copy to current %a" (Irmin.Type.pp K.t) branch);
                Irmin_layers.Stats.copy_branches ();
                U.set current branch hash
            | false -> Lwt.return_unit))
      branches

  let flip_upper t =
    Log.debug (fun l -> l "[branches] flip to %s" (log_previous_upper t));
    t.flip <- not t.flip

  (** After clearing the previous upper, we also needs to flush current upper to
      disk, otherwise values are not found by the RO. *)
  let clear_previous_upper t =
    let current = current_upper t in
    let previous = previous_upper t in
    U.clear previous >|= fun () ->
    U.flush current;
    match t.lower with None -> () | Some x -> L.flush x
end
