(*
 * Copyright (c) 2013-2019 Thomas Gazagnaire <thomas@gazagnaire.org>
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
include Inode_intf

let src =
  Logs.Src.create "irmin.pack.i" ~doc:"inodes for the irmin-pack backend"

module Log = (val Logs.src_log src : Logs.LOG)

let rec drop n (l : 'a Seq.t) () =
  match l () with
  | l' when n = 0 -> l'
  | Nil -> Nil
  | Cons (_, l') -> drop (n - 1) l' ()

let take : type a. int -> a Seq.t -> a list =
  let rec aux acc n (l : a Seq.t) =
    if n = 0 then acc
    else
      match l () with Nil -> acc | Cons (x, l') -> aux (x :: acc) (n - 1) l'
  in
  fun n s -> List.rev (aux [] n s)

module Make_intermediate
    (Conf : Config.S)
    (H : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S with type hash = H.t) =
struct
  module Node = struct
    include Node
    module H = Irmin.Hash.Typed (H) (Node)

    let hash = H.hash
  end

  module T = struct
    type hash = H.t [@@deriving irmin]
    type step = Node.step [@@deriving irmin]
    type metadata = Node.metadata [@@deriving irmin]

    let default = Node.default

    type value = Node.value

    let value_t = Node.value_t
    let pp_hash = Irmin.Type.(pp hash_t)
  end

  module StepMap = struct
    include Map.Make (struct
      type t = T.step

      let compare = Irmin.Type.(unstage (compare T.step_t))
    end)

    let of_list l = List.fold_left (fun acc (k, v) -> add k v acc) empty l
  end

  (* Binary representation, useful to compute hashes *)
  module Bin = struct
    open T

    type ptr = { index : int; hash : H.t }
    type tree = { depth : int; length : int; entries : ptr list }
    type v = Values of (step * value) list | Tree of tree

    let ptr_t : ptr Irmin.Type.t =
      let open Irmin.Type in
      record "Bin.ptr" (fun index hash -> { index; hash })
      |+ field "index" int (fun t -> t.index)
      |+ field "hash" H.t (fun (t : ptr) -> t.hash)
      |> sealr

    let tree_t : tree Irmin.Type.t =
      let open Irmin.Type in
      record "Bin.tree" (fun depth length entries -> { depth; length; entries })
      |+ field "depth" int (fun t -> t.depth)
      |+ field "length" int (fun t -> t.length)
      |+ field "entries" (list ptr_t) (fun t -> t.entries)
      |> sealr

    let v_t : v Irmin.Type.t =
      let open Irmin.Type in
      variant "Bin.v" (fun values tree -> function
        | Values l -> values l | Tree i -> tree i)
      |~ case1 "Values" (list (pair step_t value_t)) (fun t -> Values t)
      |~ case1 "Tree" tree_t (fun t -> Tree t)
      |> sealv

    module V =
      Irmin.Hash.Typed
        (H)
        (struct
          type t = v

          let t = v_t
        end)

    type t = { hash : H.t Lazy.t; stable : bool; v : v }

    let pre_hash_v = Irmin.Type.(unstage (pre_hash v_t))

    let t : t Irmin.Type.t =
      let open Irmin.Type in
      let pre_hash = stage (fun x -> pre_hash_v x.v) in
      record "Bin.t" (fun hash stable v -> { hash = lazy hash; stable; v })
      |+ field "hash" H.t (fun t -> Lazy.force t.hash)
      |+ field "stable" bool (fun t -> t.stable)
      |+ field "v" v_t (fun t -> t.v)
      |> sealr
      |> like ~pre_hash

    let v ~stable ~hash v = { stable; hash; v }
    let hash t = Lazy.force t.hash
  end

  (* Compressed binary representation *)
  module Compress = struct
    open T

    type name = Indirect of int | Direct of step
    type address = Indirect of int64 | Direct of H.t

    let address_t : address Irmin.Type.t =
      let open Irmin.Type in
      variant "Compress.address" (fun i d -> function
        | Indirect x -> i x | Direct x -> d x)
      |~ case1 "Indirect" int64 (fun x -> Indirect x)
      |~ case1 "Direct" H.t (fun x -> Direct x)
      |> sealv

    type ptr = { index : int; hash : address }

    let ptr_t : ptr Irmin.Type.t =
      let open Irmin.Type in
      record "Compress.ptr" (fun index hash -> { index; hash })
      |+ field "index" int (fun t -> t.index)
      |+ field "hash" address_t (fun t -> t.hash)
      |> sealr

    type tree = { depth : int; length : int; entries : ptr list }

    let tree_t : tree Irmin.Type.t =
      let open Irmin.Type in
      record "Compress.tree" (fun depth length entries ->
          { depth; length; entries })
      |+ field "depth" int (fun t -> t.depth)
      |+ field "length" int (fun t -> t.length)
      |+ field "entries" (list ptr_t) (fun t -> t.entries)
      |> sealr

    type value =
      | Contents of name * address * metadata
      | Node of name * address

    let is_default = Irmin.Type.(unstage (equal T.metadata_t)) T.default

    let value_t : value Irmin.Type.t =
      let open Irmin.Type in
      variant "Compress.value"
        (fun
          contents_ii
          contents_x_ii
          node_ii
          contents_id
          contents_x_id
          node_id
          contents_di
          contents_x_di
          node_di
          contents_dd
          contents_x_dd
          node_dd
        -> function
        | Contents (Indirect n, Indirect h, m) ->
            if is_default m then contents_ii (n, h) else contents_x_ii (n, h, m)
        | Node (Indirect n, Indirect h) -> node_ii (n, h)
        | Contents (Indirect n, Direct h, m) ->
            if is_default m then contents_id (n, h) else contents_x_id (n, h, m)
        | Node (Indirect n, Direct h) -> node_id (n, h)
        | Contents (Direct n, Indirect h, m) ->
            if is_default m then contents_di (n, h) else contents_x_di (n, h, m)
        | Node (Direct n, Indirect h) -> node_di (n, h)
        | Contents (Direct n, Direct h, m) ->
            if is_default m then contents_dd (n, h) else contents_x_dd (n, h, m)
        | Node (Direct n, Direct h) -> node_dd (n, h))
      |~ case1 "contents-ii" (pair int int64) (fun (n, i) ->
             Contents (Indirect n, Indirect i, T.default))
      |~ case1 "contents-x-ii" (triple int int64 metadata_t) (fun (n, i, m) ->
             Contents (Indirect n, Indirect i, m))
      |~ case1 "node-ii" (pair int int64) (fun (n, i) ->
             Node (Indirect n, Indirect i))
      |~ case1 "contents-id" (pair int H.t) (fun (n, h) ->
             Contents (Indirect n, Direct h, T.default))
      |~ case1 "contents-x-id" (triple int H.t metadata_t) (fun (n, h, m) ->
             Contents (Indirect n, Direct h, m))
      |~ case1 "node-id" (pair int H.t) (fun (n, h) ->
             Node (Indirect n, Direct h))
      |~ case1 "contents-di" (pair step_t int64) (fun (n, i) ->
             Contents (Direct n, Indirect i, T.default))
      |~ case1 "contents-x-di" (triple step_t int64 metadata_t)
           (fun (n, i, m) -> Contents (Direct n, Indirect i, m))
      |~ case1 "node-di" (pair step_t int64) (fun (n, i) ->
             Node (Direct n, Indirect i))
      |~ case1 "contents-dd" (pair step_t H.t) (fun (n, i) ->
             Contents (Direct n, Direct i, T.default))
      |~ case1 "contents-x-dd" (triple step_t H.t metadata_t) (fun (n, i, m) ->
             Contents (Direct n, Direct i, m))
      |~ case1 "node-dd" (pair step_t H.t) (fun (n, i) ->
             Node (Direct n, Direct i))
      |> sealv

    type v = Values of value list | Tree of tree

    let v_t : v Irmin.Type.t =
      let open Irmin.Type in
      variant "Compress.v" (fun values tree -> function
        | Values x -> values x | Tree x -> tree x)
      |~ case1 "Values" (list value_t) (fun x -> Values x)
      |~ case1 "Tree" tree_t (fun x -> Tree x)
      |> sealv

    type t = { hash : H.t; stable : bool; v : v }

    let v ~stable ~hash v = { hash; stable; v }
    let magic_node = 'N'
    let magic_inode = 'I'

    let stable_t : bool Irmin.Type.t =
      Irmin.Type.(map char)
        (fun n -> n = magic_node)
        (function true -> magic_node | false -> magic_inode)

    let t =
      let open Irmin.Type in
      record "Compress.t" (fun hash stable v -> { hash; stable; v })
      |+ field "hash" H.t (fun t -> t.hash)
      |+ field "stable" stable_t (fun t -> t.stable)
      |+ field "v" v_t (fun t -> t.v)
      |> sealr
  end

  module Val_impl = struct
    open T

    let equal_value = Irmin.Type.(unstage (equal value_t))

    type 'a layout =
      | Partial : (hash -> lazy_ptr t option) -> lazy_ptr layout
      | Total : direct_ptr layout
      | Flaky : lazy_ptr layout

    and lazy_ptr = {
      target_hash : hash Lazy.t;
      mutable target : lazy_ptr t option;
    }

    and direct_ptr = Ptr of direct_ptr t [@@unboxed]

    and 'ptr tree = { depth : int; length : int; entries : 'ptr option array }

    and 'ptr v = Values of value StepMap.t | Tree of 'ptr tree

    and 'ptr t = { hash : hash Lazy.t; stable : bool; v : 'ptr v }

    module Ptr = struct
      let hash : type ptr. ptr layout -> ptr -> _ = function
        | Total -> fun (Ptr ptr) -> Lazy.force ptr.hash
        | Partial _ -> fun { target_hash; _ } -> Lazy.force target_hash
        | Flaky -> fun { target_hash; _ } -> Lazy.force target_hash

      let target : type ptr. ptr layout -> ptr -> ptr t =
       fun la ->
        match la with
        | Total -> fun (Ptr t) -> t
        | Partial find -> (
            function
            | { target = Some entry; _ } -> entry
            | t -> (
                let h = hash la t in
                match find h with
                | None -> Fmt.failwith "%a: unknown key" pp_hash h
                | Some x ->
                    t.target <- Some x;
                    x))
        | Flaky -> (
            function
            | { target = Some entry; _ } -> entry
            | _ ->
                failwith
                  "Impossible to load the subtree on an inode unserialized \
                   using Repr")

      let of_target : type ptr. ptr layout -> ptr t -> ptr = function
        | Total -> fun target -> Ptr target
        | Partial _ ->
            fun target -> { target = Some target; target_hash = target.hash }
        | Flaky ->
            fun target -> { target = Some target; target_hash = target.hash }

      let iter : type ptr. ptr layout -> (ptr t -> unit) -> ptr -> unit =
        function
        | Total -> fun f (Ptr entry) -> f entry
        | Partial _ -> (
            fun f -> function { target = Some entry; _ } -> f entry | _ -> ())
        | Flaky -> (
            fun f -> function { target = Some entry; _ } -> f entry | _ -> ())
    end

    let pred la t =
      match t.v with
      | Tree i ->
          let hash_of_ptr = Ptr.hash la in
          Array.fold_left
            (fun acc -> function
              | None -> acc
              | Some ptr -> `Inode (hash_of_ptr ptr) :: acc)
            [] i.entries
      | Values l ->
          StepMap.fold
            (fun _ v acc ->
              let v =
                match v with
                | `Node _ as k -> k
                | `Contents (k, _) -> `Contents k
              in
              v :: acc)
            l []

    let length t =
      match t.v with Values vs -> StepMap.cardinal vs | Tree vs -> vs.length

    let stable t = t.stable

    type acc = {
      cursor : int;
      values : (step * value) list list;
      remaining : int;
    }

    let empty_acc n = { cursor = 0; values = []; remaining = n }

    let rec list_entry la ~offset ~length acc = function
      | None -> acc
      | Some i -> list_values la ~offset ~length acc (Ptr.target la i)

    and list_tree la ~offset ~length acc t =
      if acc.remaining <= 0 || offset + length <= acc.cursor then acc
      else if acc.cursor + t.length < offset then
        { acc with cursor = t.length + acc.cursor }
      else Array.fold_left (list_entry la ~offset ~length) acc t.entries

    and list_values la ~offset ~length acc t =
      if acc.remaining <= 0 || offset + length <= acc.cursor then acc
      else
        match t.v with
        | Values vs ->
            let len = StepMap.cardinal vs in
            if acc.cursor + len < offset then
              { acc with cursor = len + acc.cursor }
            else
              let to_drop =
                if acc.cursor > offset then 0 else offset - acc.cursor
              in
              let vs =
                StepMap.to_seq vs |> drop to_drop |> take acc.remaining
              in
              let n = List.length vs in
              {
                values = vs :: acc.values;
                cursor = acc.cursor + len;
                remaining = acc.remaining - n;
              }
        | Tree t -> list_tree la ~offset ~length acc t

    let list la ?(offset = 0) ?length t =
      let length =
        match length with
        | Some n -> n
        | None -> (
            match t.v with
            | Values vs -> StepMap.cardinal vs - offset
            | Tree i -> i.length - offset)
      in
      let entries = list_values la ~offset ~length (empty_acc length) t in
      List.concat (List.rev entries.values)

    let to_bin_v la = function
      | Values vs ->
          let vs = StepMap.bindings vs in
          Bin.Values vs
      | Tree t ->
          let hash_of_ptr = Ptr.hash la in
          let _, entries =
            Array.fold_left
              (fun (i, acc) -> function
                | None -> (i + 1, acc)
                | Some ptr ->
                    let hash = hash_of_ptr ptr in
                    (i + 1, { Bin.index = i; hash } :: acc))
              (0, []) t.entries
          in
          let entries = List.rev entries in
          Bin.Tree { depth = t.depth; length = t.length; entries }

    let to_bin la t =
      let v = to_bin_v la t.v in
      Bin.v ~stable:t.stable ~hash:t.hash v

    let hash t = Lazy.force t.hash

    let stabilize la t =
      if t.stable then t
      else
        let n = length t in
        if n > Conf.stable_hash then t
        else
          let hash =
            lazy
              (let vs = list la t in
               Node.hash (Node.v vs))
          in
          { hash; stable = true; v = t.v }

    let hash_key = Irmin.Type.(unstage (short_hash step_t))
    let index ~depth k = abs (hash_key ~seed:depth k) mod Conf.entries

    let of_bin t =
      let v =
        match t.Bin.v with
        | Bin.Values vs ->
            let vs = StepMap.of_list vs in
            Values vs
        | Tree t ->
            let entries = Array.make Conf.entries None in
            List.iter
              (fun { Bin.index; hash } ->
                entries.(index) <-
                  Some { target = None; target_hash = lazy hash })
              t.entries;
            Tree { depth = t.Bin.depth; length = t.length; entries }
      in
      { hash = t.Bin.hash; stable = t.Bin.stable; v }

    let empty : 'a layout -> 'a t =
     fun _ ->
      let hash = lazy (Node.hash Node.empty) in
      { stable = true; hash; v = Values StepMap.empty }

    let values : 'a layout -> _ -> 'a t =
     fun la vs ->
      let length = StepMap.cardinal vs in
      if length = 0 then empty la
      else
        let v = Values vs in
        let hash = lazy (Bin.V.hash (to_bin_v la v)) in
        { hash; stable = false; v }

    let tree la is =
      let v = Tree is in
      let hash = lazy (Bin.V.hash (to_bin_v la v)) in
      { hash; stable = false; v }

    let of_values la l = values la (StepMap.of_list l)

    let is_empty t =
      match t.v with Values vs -> StepMap.is_empty vs | Tree _ -> false

    let find_value la ~depth t s =
      let target_of_ptr = Ptr.target la in
      let rec aux ~depth = function
        | Values vs -> ( try Some (StepMap.find s vs) with Not_found -> None)
        | Tree t -> (
            let i = index ~depth s in
            let x = t.entries.(i) in
            match x with
            | None -> None
            | Some i -> aux ~depth:(depth + 1) (target_of_ptr i).v)
      in
      aux ~depth t.v

    let find la t s = find_value ~depth:0 la t s

    let rec add la ~depth ~copy ~replace t s v k =
      match t.v with
      | Values vs ->
          let length =
            if replace then StepMap.cardinal vs else StepMap.cardinal vs + 1
          in
          let t =
            if length <= Conf.entries then values la (StepMap.add s v vs)
            else
              let vs = StepMap.bindings (StepMap.add s v vs) in
              let empty =
                tree la
                  { length = 0; depth; entries = Array.make Conf.entries None }
              in
              let aux t (s, v) =
                (add [@tailcall]) la ~depth ~copy:false ~replace t s v (fun x ->
                    x)
              in
              List.fold_left aux empty vs
          in
          k t
      | Tree t -> (
          let length = if replace then t.length else t.length + 1 in
          let entries = if copy then Array.copy t.entries else t.entries in
          let i = index ~depth s in
          match entries.(i) with
          | None ->
              let target = values la (StepMap.singleton s v) in
              entries.(i) <- Some (Ptr.of_target la target);
              let t = tree la { depth; length; entries } in
              k t
          | Some n ->
              let t = Ptr.target la n in
              add la ~depth:(depth + 1) ~copy ~replace t s v @@ fun target ->
              entries.(i) <- Some (Ptr.of_target la target);
              let t = tree la { depth; length; entries } in
              k t)

    let add la ~copy t s v =
      (* XXX: [find_value ~depth:42] should break the unit tests. It doesn't. *)
      match find_value ~depth:0 la t s with
      | Some v' when equal_value v v' -> stabilize la t
      | Some _ -> add ~depth:0 la ~copy ~replace:true t s v (stabilize la)
      | None -> add ~depth:0 la ~copy ~replace:false t s v (stabilize la)

    let rec remove la ~depth t s k =
      match t.v with
      | Values vs ->
          let t = values la (StepMap.remove s vs) in
          k t
      | Tree t -> (
          let len = t.length - 1 in
          if len <= Conf.entries then
            let vs =
              list_tree la ~offset:0 ~length:t.length (empty_acc t.length) t
            in
            let vs = List.concat (List.rev vs.values) in
            let vs = StepMap.of_list vs in
            let vs = StepMap.remove s vs in
            let t = values la vs in
            k t
          else
            let entries = Array.copy t.entries in
            let i = index ~depth s in
            match entries.(i) with
            | None -> assert false
            | Some t ->
                let t = Ptr.target la t in
                if length t = 1 then (
                  entries.(i) <- None;
                  let t = tree la { depth; length = len; entries } in
                  k t)
                else
                  remove ~depth:(depth + 1) la t s @@ fun target ->
                  entries.(i) <- Some (Ptr.of_target la target);
                  let t = tree la { depth; length = len; entries } in
                  k t)

    let remove la t s =
      (* XXX: [find_value ~depth:42] should break the unit tests. It doesn't. *)
      match find_value la ~depth:0 t s with
      | None -> stabilize la t
      | Some _ -> remove la ~depth:0 t s (stabilize la)

    let v l =
      let len = List.length l in
      let t =
        if len <= Conf.entries then of_values Total l
        else
          let aux acc (s, v) = add Total ~copy:false acc s v in
          List.fold_left aux (empty Total) l
      in
      stabilize Total t

    let save la ~add ~mem t =
      let iter_entries =
        let iter_ptr = Ptr.iter la in
        fun f arr -> Array.iter (Option.iter (iter_ptr f)) arr
      in
      let rec aux ~depth t =
        Log.debug (fun l -> l "save depth:%d" depth);
        match t.v with
        | Values _ -> add (Lazy.force t.hash) (to_bin la t)
        | Tree n ->
            iter_entries
              (fun t ->
                let hash = Lazy.force t.hash in
                if mem hash then () else aux ~depth:(depth + 1) t)
              n.entries;
            add (Lazy.force t.hash) (to_bin la t)
      in
      aux ~depth:0 t
  end

  module Elt = struct
    type t = Bin.t

    let t = Bin.t

    let magic (t : t) =
      if t.stable then Compress.magic_node else Compress.magic_inode

    let hash t = Bin.hash t
    let step_to_bin = Irmin.Type.(unstage (to_bin_string T.step_t))
    let step_of_bin = Irmin.Type.(unstage (of_bin_string T.step_t))
    let encode_compress = Irmin.Type.(unstage (encode_bin Compress.t))
    let decode_compress = Irmin.Type.(unstage (decode_bin Compress.t))

    let encode_bin ~dict ~offset (t : t) k =
      let step s : Compress.name =
        let str = step_to_bin s in
        if String.length str <= 3 then Direct s
        else match dict str with Some i -> Indirect i | None -> Direct s
      in
      let hash h : Compress.address =
        match offset h with
        | None -> Compress.Direct h
        | Some off -> Compress.Indirect off
      in
      let ptr : Bin.ptr -> Compress.ptr =
       fun n ->
        let hash = hash n.hash in
        { index = n.index; hash }
      in
      let value : T.step * T.value -> Compress.value = function
        | s, `Contents (c, m) ->
            let s = step s in
            let v = hash c in
            Compress.Contents (s, v, m)
        | s, `Node n ->
            let s = step s in
            let v = hash n in
            Compress.Node (s, v)
      in
      (* List.map is fine here as the number of entries is small *)
      let v : Bin.v -> Compress.v = function
        | Values vs -> Values (List.map value vs)
        | Tree { depth; length; entries } ->
            let entries = List.map ptr entries in
            Tree { Compress.depth; length; entries }
      in
      let t = Compress.v ~stable:t.stable ~hash:k (v t.v) in
      encode_compress t

    exception Exit of [ `Msg of string ]

    let decode_bin_with_offset ~dict ~hash t off : int * t =
      let off, i = decode_compress t off in
      let step : Compress.name -> T.step = function
        | Direct n -> n
        | Indirect s -> (
            match dict s with
            | None -> raise_notrace (Exit (`Msg "dict"))
            | Some s -> (
                match step_of_bin s with
                | Error e -> raise_notrace (Exit e)
                | Ok v -> v))
      in
      let hash : Compress.address -> H.t = function
        | Indirect off -> hash off
        | Direct n -> n
      in
      let ptr : Compress.ptr -> Bin.ptr =
       fun n ->
        let hash = hash n.hash in
        { index = n.index; hash }
      in
      let value : Compress.value -> T.step * T.value = function
        | Contents (n, h, metadata) ->
            let name = step n in
            let hash = hash h in
            (name, `Contents (hash, metadata))
        | Node (n, h) ->
            let name = step n in
            let hash = hash h in
            (name, `Node hash)
      in
      let t : Compress.v -> Bin.v = function
        | Values vs -> Values (List.rev_map value (List.rev vs))
        | Tree { depth; length; entries } ->
            let entries = List.map ptr entries in
            Tree { depth; length; entries }
      in
      let t = Bin.v ~stable:i.stable ~hash:(lazy i.hash) (t i.v) in
      (off, t)

    let decode_bin ~dict ~hash t off =
      decode_bin_with_offset ~dict ~hash t off |> snd
  end

  type hash = T.hash

  let pp_hash = T.pp_hash

  let decode_bin ~dict ~hash t off =
    Elt.decode_bin_with_offset ~dict ~hash t off

  module Val = struct
    include T
    module I = Val_impl

    type t =
      | Partial of I.lazy_ptr I.layout * I.lazy_ptr I.t
      | Total of I.direct_ptr I.layout * I.direct_ptr I.t
      | Flaky of I.lazy_ptr I.layout * I.lazy_ptr I.t

    type 'b apply_fn = { f : 'a. 'a I.layout -> 'a I.t -> 'b } [@@unboxed]

    let apply : t -> 'b apply_fn -> 'b =
     fun t f ->
      match t with
      | Partial (la, v) -> f.f la v
      | Total (la, v) -> f.f la v
      | Flaky (la, v) -> f.f la v

    type map_fn = { f : 'a. 'a I.layout -> 'a I.t -> 'a I.t } [@@unboxed]

    let map : t -> map_fn -> t =
     fun t f ->
      match t with
      | Partial (la, v) ->
          let v' = f.f la v in
          if v == v' then t else Partial (la, v')
      | Total (la, v) ->
          let v' = f.f la v in
          if v == v' then t else Total (la, v')
      | Flaky (la, v) ->
          let v' = f.f la v in
          if v == v' then t else Flaky (la, v')

    let pred t = apply t { f = (fun la v -> I.pred la v) }
    let v l = Total (I.Total, I.v l)

    let list ?offset ?length t =
      apply t { f = (fun la v -> I.list la ?offset ?length v) }

    let empty = v []
    let is_empty t = apply t { f = (fun _ v -> I.is_empty v) }
    let find t s = apply t { f = (fun la v -> I.find la v s) }

    let add t s value =
      map t { f = (fun la v -> I.add ~copy:true la v s value) }

    let remove t s = map t { f = (fun la v -> I.remove la v s) }
    let pre_hash_binv = Irmin.Type.(unstage (pre_hash Bin.v_t))
    let pre_hash_node = Irmin.Type.(unstage (pre_hash Node.t))

    let t : t Irmin.Type.t =
      let pre_hash =
        Irmin.Type.stage @@ fun x ->
        let stable = apply x { f = (fun _ v -> I.stable v) } in
        if not stable then
          let bin = apply x { f = (fun la v -> I.to_bin la v) } in
          pre_hash_binv bin.v
        else
          let vs = list x in
          pre_hash_node (Node.v vs)
      in
      Irmin.Type.map ~pre_hash Bin.t
        (fun bin -> Flaky (Val_impl.Flaky, I.of_bin bin))
        (fun x -> apply x { f = (fun la v -> I.to_bin la v) })

    let hash t = apply t { f = (fun _ v -> I.hash v) }
    let save ~add ~mem t = apply t { f = (fun la v -> I.save la ~add ~mem v) }

    let of_bin find v =
      let find h =
        match find h with None -> None | Some v -> Some (I.of_bin v)
      in
      Partial (I.Partial find, I.of_bin v)

    let to_bin t = apply t { f = (fun la v -> I.to_bin la v) }
    let stable t = apply t { f = (fun _ v -> I.stable v) }
    let length t = apply t { f = (fun _ v -> I.length v) }
    let index = I.index
  end
end

module Make_ext
    (H : Irmin.Hash.S)
    (Node : Irmin.Private.Node.S with type hash = H.t)
    (Inter : INTER
               with type hash = H.t
                and type Val.metadata = Node.metadata
                and type Val.step = Node.step)
    (P : Pack.MAKER with type key = H.t and type index = Pack_index.Make(H).t) =
struct
  module Key = H
  module Pack = P.Make (Inter.Elt)

  type 'a t = 'a Pack.t
  type key = Key.t
  type value = Inter.Val.t
  type index = Pack.index

  let mem t k = Pack.mem t k

  let find t k =
    Pack.find t k >|= function
    | None -> None
    | Some v ->
        let find = Pack.unsafe_find ~check_integrity:true t in
        let v = Inter.Val.of_bin find v in
        Some v

  let save t v =
    let add k v =
      Pack.unsafe_append ~ensure_unique:true ~overcommit:false t k v
    in
    Inter.Val.save ~add ~mem:(Pack.unsafe_mem t) v

  let hash v = Inter.Val.hash v

  let add t v =
    save t v;
    Lwt.return (hash v)

  let equal_hash = Irmin.Type.(unstage (equal H.t))

  let check_hash expected got =
    if equal_hash expected got then ()
    else
      Fmt.invalid_arg "corrupted value: got %a, expecting %a" Inter.pp_hash
        expected Inter.pp_hash got

  let unsafe_add t k v =
    check_hash k (hash v);
    save t v;
    Lwt.return_unit

  let batch = Pack.batch
  let v = Pack.v
  let integrity_check = Pack.integrity_check
  let close = Pack.close
  let sync = Pack.sync
  let clear = Pack.clear
  let clear_caches = Pack.clear_caches

  let decode_bin ~dict ~hash buff off =
    Inter.decode_bin ~dict ~hash buff off |> fst

  module Val = Inter.Val
end

module Make
    (Conf : Config.S)
    (H : Irmin.Hash.S)
    (P : Pack.MAKER with type key = H.t and type index = Pack_index.Make(H).t)
    (Node : Irmin.Private.Node.S with type hash = H.t) =
struct
  module Inter = Make_intermediate (Conf) (H) (Node)
  include Make_ext (H) (Node) (Inter) (P)
end
