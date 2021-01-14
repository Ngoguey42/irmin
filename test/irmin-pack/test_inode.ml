open Lwt.Infix
open Common

let root = Filename.concat "_build" "test-inode"
let src = Logs.Src.create "tests.instances" ~doc:"Tests"

module Log = (val Logs.src_log src : Logs.LOG)

module Conf = struct
  let entries = 2
  let stable_hash = 3
end

let log_size = 1000

module Path = Irmin.Path.String_list
module Metadata = Irmin.Metadata.None
module Node = Irmin.Private.Node.Make (H) (Path) (Metadata)
module Index = Irmin_pack.Index.Make (H)
module Inode = Irmin_pack.Inode.Make (Conf) (H) (P) (Node)
module Private = Inode.Val.Private

module Context = struct
  type t = {
    index : Index.t;
    store : [ `Read ] Inode.t;
    clone : readonly:bool -> [ `Read ] Inode.t Lwt.t;
  }

  let get_store ?(lru_size = 0) () =
    rm_dir root;
    let index = Index.v ~log_size ~fresh:true root in
    Inode.v ~fresh:true ~lru_size ~index root >|= fun store ->
    let clone ~readonly =
      Inode.v ~lru_size ~fresh:false ~readonly ~index root
    in

    { index; store; clone }

  let close t =
    Index.close t.index;
    Inode.close t.store
end

module H_contents =
  Irmin.Hash.Typed
    (H)
    (struct
      type t = string

      let t = Irmin.Type.string
    end)

module String_set = Set.Make (struct
  type t = string

  let compare = compare
end)

let normal x = `Contents (x, Metadata.default)
let foo = H_contents.hash "foo"
let bar = H_contents.hash "bar"
let check_hash = Alcotest.check_repr Inode.Val.hash_t
let check_values = Alcotest.check_repr Inode.Val.t

(** Use brute force to generate a [step] from a list of index that this [step]
    should map to through the [Private.index] function. *)
let gen_step : int list -> Path.step =
  let tbl = Hashtbl.create 10 in
  let max_brute_force_iterations = 100 in
  fun indices ->
    let step_of_small_int i =
      Char.code 'a' + (i mod 26) |> Char.chr |> Char.escaped
    in
    let rec step_of_any_int i =
      if i < 26 then step_of_small_int i
      else step_of_small_int (i mod 26) ^ step_of_any_int (i / 26)
    in
    let rec aux i =
      if i > max_brute_force_iterations then
        failwith "Could not quickly generate a step"
      else
        let s = step_of_any_int i in
        let is_valid =
          indices
          |> List.mapi (fun depth i -> (depth, i))
          |> List.for_all (fun (depth, i) -> Private.index ~seed:depth s = i)
        in
        if is_valid then s else aux (i + 1)
    in
    match Hashtbl.find_opt tbl indices with
    | Some s -> s
    | None ->
        let s = aux 0 in
        Hashtbl.add tbl indices s;
        s

(** List all the steps that would fill a tree of depth [maxdepth_of_test]. *)
let gen_steps maxdepth_of_test : Path.step list =
  let ( ** ) a b = float_of_int a ** float_of_int b |> int_of_float in
  List.init (Conf.entries ** maxdepth_of_test) (fun i ->
      List.init maxdepth_of_test (fun j ->
          let j = Conf.entries ** (maxdepth_of_test - j - 1) in
          (* In the binary case (Conf.entries = 2), [j] is now the mask of the
             bit to look at *)
          i / j mod Conf.entries))
  |> List.map gen_step

let powerset xs =
  List.fold_left (fun acc x -> acc @ List.map (fun ys -> x :: ys) acc) [ [] ] xs

let check_node msg v t =
  let h = Private.hash v in
  Inode.batch t.Context.store (fun i -> Inode.add i v) >|= fun h' ->
  check_hash msg h h'

let check_hardcoded_hash msg h v =
  h |> Irmin.Type.of_string Inode.Val.hash_t |> function
  | Error (`Msg str) -> Alcotest.failf "hash of string failed: %s" str
  | Ok hash -> check_hash msg hash (Private.hash v)

(** Test add values from an empty node. *)
let test_add_values () =
  rm_dir root;
  Context.get_store () >>= fun t ->
  check_node "hash empty node" Inode.Val.empty t >>= fun () ->
  let v1 = Inode.Val.add Inode.Val.empty "x" (normal foo) in
  let v2 = Inode.Val.add v1 "y" (normal bar) in
  check_node "node x+y" v2 t >>= fun () ->
  check_hardcoded_hash "hash v2" "d4b55db5d2d806283766354f0d7597d332156f74" v2;
  let v3 = Inode.Val.v [ ("x", normal foo); ("y", normal bar) ] in
  check_values "add x+y vs v x+y" v2 v3;
  Context.close t

(** Test add to inodes. *)
let test_add_inodes () =
  rm_dir root;
  Context.get_store () >>= fun t ->
  let v1 = Inode.Val.v [ ("x", normal foo); ("y", normal bar) ] in
  let v2 = Inode.Val.add v1 "z" (normal foo) in
  let v3 =
    Inode.Val.v [ ("x", normal foo); ("z", normal foo); ("y", normal bar) ]
  in
  check_values "add x+y+z vs v x+z+y" v2 v3;
  check_hardcoded_hash "hash v3" "46fe6c68a11a6ecd14cbe2d15519b6e5f3ba2864" v3;
  Alcotest.(check bool) "v1 stable" (Private.stable v1) true;
  Alcotest.(check bool) "v2 stable" (Private.stable v2) true;
  let v4 = Inode.Val.add v2 "a" (normal foo) in
  let v5 =
    Inode.Val.v
      [
        ("x", normal foo);
        ("z", normal foo);
        ("a", normal foo);
        ("y", normal bar);
      ]
  in
  check_values "add x+y+z+a vs v x+z+a+y" v4 v5;
  check_hardcoded_hash "hash v4" "c330c08571d088141dfc82f644bffcfcf6696539" v4;
  Alcotest.(check bool) "v4 not stable" (Private.stable v4) false;
  Context.close t

(** Test remove values on an empty node. *)
let test_remove_values () =
  rm_dir root;
  Context.get_store () >>= fun t ->
  let v1 = Inode.Val.v [ ("x", normal foo); ("y", normal bar) ] in
  let v2 = Inode.Val.remove v1 "y" in
  let v3 = Inode.Val.v [ ("x", normal foo) ] in
  check_values "node x obtained two ways" v2 v3;
  check_hardcoded_hash "hash v2" "a1996f4309ea31cc7ba2d4c81012885aa0e08789" v2;
  let v4 = Inode.Val.remove v2 "x" in
  check_node "remove results in an empty node" Inode.Val.empty t >>= fun () ->
  let v5 = Inode.Val.remove v4 "x" in
  check_values "remove on an already empty node" v4 v5;
  check_hardcoded_hash "hash v4" "5ba93c9db0cff93f52b521d7420e43f6eda2784f" v4;
  Alcotest.(check bool) "v5 is empty" (Inode.Val.is_empty v5) true;
  Context.close t

(** Test remove and add values to go from stable to unstable inodes. *)
let test_remove_inodes () =
  rm_dir root;
  Context.get_store () >>= fun t ->
  let v1 =
    Inode.Val.v [ ("x", normal foo); ("y", normal bar); ("z", normal foo) ]
  in
  check_hardcoded_hash "hash v1" "46fe6c68a11a6ecd14cbe2d15519b6e5f3ba2864" v1;
  let v2 = Inode.Val.remove v1 "x" in
  let v3 = Inode.Val.v [ ("y", normal bar); ("z", normal foo) ] in
  check_values "node y+z obtained two ways" v2 v3;
  check_hardcoded_hash "hash v2" "ea22a2936eed53978bde62f0185cee9d8bbf9489" v2;
  let v4 =
    Inode.Val.v
      [
        ("x", normal foo);
        ("z", normal foo);
        ("a", normal foo);
        ("y", normal bar);
      ]
  in
  let v5 = Inode.Val.remove v4 "a" in
  let h1 = Private.hash v1 in
  let h5 = Private.hash v5 in
  check_hash
    "v1 and v5 have different internal representations but the same hash" h1 h5;
  Alcotest.(check bool) "v1 stable" (Private.stable v1) true;
  Alcotest.(check bool) "v5 stable" (Private.stable v5) true;
  Context.close t

(** [steps] is a list of length 8 containing the necessary steps to fill a tree
    of depth=3.

    [trees] is a list of length 2^8 containing the [Node.Val.t] formed from the
    powerset of [steps] and constructed using [Node.Val.v]. [trees] contains the
    empty tree and the full 8-leaves tree.

    From all the 2^8 trees, assert that independantly, the 8 possible
    [add]/[remove] operations yield a tree with its representation included in
    [trees]. *)
let test_representation_uniqueness_maxdepth_3 () =
  let steps = gen_steps 3 in
  let contents = List.map (fun s -> H_contents.hash s |> normal) steps in
  let leaves_per_tree = powerset (List.combine steps contents) in
  Alcotest.(check int) "Size of the powerset" (List.length leaves_per_tree) 256;
  let trees = List.map Inode.Val.v leaves_per_tree in
  let repr_per_tree =
    trees |> List.map (Repr.to_string Inode.Val.t) |> String_set.of_list
  in
  let f tree s c =
    match Inode.Val.find tree s with
    | Some _ ->
        let tree' = Inode.Val.remove tree s in
        let repr' = Repr.to_string Inode.Val.t tree' in
        if not @@ String_set.mem repr' repr_per_tree then
          Alcotest.failf "%s can be built with both [v] and [remove]" repr'
    | None ->
        let tree' = Inode.Val.add tree s c in
        let repr' = Repr.to_string Inode.Val.t tree' in
        if not @@ String_set.mem repr' repr_per_tree then
          Alcotest.failf "%s can be built with both [v] and [add]" repr'
  in
  List.iter (fun t -> List.iter2 (fun s c -> f t s c) steps contents) trees;
  Lwt.return_unit

let tests =
  [
    Alcotest.test_case "add values" `Quick (fun () ->
        Lwt_main.run (test_add_values ()));
    Alcotest.test_case "add values to inodes" `Quick (fun () ->
        Lwt_main.run (test_add_inodes ()));
    Alcotest.test_case "remove values" `Quick (fun () ->
        Lwt_main.run (test_remove_values ()));
    Alcotest.test_case "remove inodes" `Quick (fun () ->
        Lwt_main.run (test_remove_inodes ()));
    Alcotest.test_case "test representation uniqueness" `Quick (fun () ->
        Lwt_main.run (test_representation_uniqueness_maxdepth_3 ()));
  ]
