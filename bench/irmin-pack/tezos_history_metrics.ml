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

(** List of tuples of week timestamp, week transaction count, week ops count and
    week block count.

    There is ~1 block per minute in tezos and 10080 minutes in a week. Hence the
    content of the last columns.

    Computed using:

    https://api.tzstats.com/series/block.json?collapse=1w&columns=n_tx,n_ops,count

    https://api.tzstats.com/tables/block.json?columns=height,n_ops,n_tx,n_ops_contract&height.gte=1500000 *)

open Irmin.Export_for_backends

let iter_2by2 : ('a -> 'a -> unit) -> 'a array -> unit =
 fun f a ->
  let count = Array.length a in
  for i = 0 to count - 2 do
    f a.(i) a.(i + 1)
  done

(* The very first will be kept *)
let filter_2by2 : ('a -> 'a -> bool) -> 'a list -> 'a list =
  fun f l ->
  match l with
  | [] | [ _ ] -> l
  | hd :: tl ->
     let rec aux prev l acc =
       match l with
       | hd :: tl when f prev hd -> aux hd tl (hd :: acc)
       | hd :: tl -> aux hd tl acc
       | [] -> List.rev acc
     in
     aux hd tl [ hd ]

let request_size = 10000

type block_info = {
  op_count : int;
  op_count_tx : int;
  op_count_contract : int;
  hash : string;
}

type t = { root : string; tbl : (int, block_info) Hashtbl.t }

(* Using yojson because repr doesn't handle tuples of size >=4 *)
type page = (int * int * int * int * string) array
[@@deriving yojson { exn = true }]

(* fetches on the tzstats API *)
let fetch page_i : string Lwt.t =
  let open Cohttp_lwt_unix in
  let url =
    (* The heights might not be unique. Several blocks might have same
       height. Let's crash when that happens. *)
    Printf.sprintf
      "https://api.tzstats.com/tables/block.json?columns=height,n_ops,n_tx,n_ops_contract,hash&height.gte=%d&height.lt=%d&limit=%d"
      (page_i * request_size)
      ((page_i + 1) * request_size)
      (request_size * 2)
  in
  Printf.eprintf "> Fetching %s\n%!" url;
  Lwt.pick
    [
      (Client.get (Uri.of_string url) >|= fun v -> `Done v);
      (Lwt_unix.sleep 2. >|= fun () -> `Timeout);
    ]
  >>= function
  | `Timeout -> Lwt.fail_with "Timeout expired"
  | `Done (resp, body) -> (
      match resp.Cohttp.Response.status with
      | `OK -> Cohttp_lwt.Body.to_string body
      | _ ->
          Fmt.failwith "Failed to fetch info from tzstats.com\n%a"
            Cohttp.Response.pp_hum resp)

(* mutates the filesystem (if necessary) *)
let fetch t page_i : page Lwt.t =
  let path =
    Filename.concat t.root
      (Printf.sprintf "%d-%d.json" (page_i * request_size)
         (((page_i + 1) * request_size) - 1))
  in
  if Sys.file_exists path then (
    let chan = open_in path in
    let len = in_channel_length chan in
    let page = really_input_string chan len in
    close_in chan;
    let res =
      match page |> Yojson.Safe.from_string |> page_of_yojson with
      | Ok v -> v
      | Error _ -> failwith "Couldn't deserialise json string from disk"
    in
    Lwt.return res)
  else
    fetch page_i >>= fun page ->
    let res =
      match page |> Yojson.Safe.from_string |> page_of_yojson with
      | Ok v -> v
      | Error _ -> failwith "Couldn't deserialise json string from network"
    in
    let res =
      Array.to_list res
      |> filter_2by2 (fun (h, _, _, _, _) (h', _, _, _, _) -> h <> h')
      |> Array.of_list
    in
    let count_exp = request_size in
    let count_got = Array.length res in
    let last_height_exp = ((page_i + 1) * request_size) - 1 in
    let last_height_got =
      match count_got with
      | 0 -> None
      | _ -> Some (Array.get res (count_got - 1) |> (fun (h, _, _, _, _) -> h))
    in
    iter_2by2 (fun (h, _, _, _, _) (h', _, _, _, _) ->
        if h + 1 <> h' then Fmt.failwith "Two consecutive blocks have level %d / %d from tzstats.com" h h'
      ) res;
    if count_exp = count_got then (
      assert (last_height_exp = Option.get last_height_got);
      Printf.eprintf "> Saving %s\n%!" path;
      let chan = open_out path in
      output_string chan page;
      close_out chan);
    Lwt.return res

(* mutates the hashtbl *)
let fetch t i : unit Lwt.t =
  let page_i = i / request_size in
  fetch t page_i >|= fun page ->
  if Array.length page <= i - (page_i * request_size) then
    failwith "Did not retrieve enough blocks from tzstats";
  Array.iter
    (fun (height, op_count, op_count_tx, op_count_contract, hash) ->
      Hashtbl.add t.tbl height
        { op_count; op_count_tx; op_count_contract; hash })
    page

let fetch t i : block_info Lwt.t =
  match Hashtbl.find_opt t.tbl i with
  | None -> (
      fetch t i >|= fun () ->
      match Hashtbl.find_opt t.tbl i with None -> assert false | Some v -> v)
  | Some v -> Lwt.return v

let create cache_dir_path =
  let rec mkdir_p path =
    if Sys.file_exists path then ()
    else
      let path' = Filename.dirname path in
      if path' = path then
        failwith "Failed to prepare cache dir for tezos metrics";
      mkdir_p path';
      Unix.mkdir path 0o755
  in
  mkdir_p cache_dir_path;
  { root = cache_dir_path; tbl = Hashtbl.create 1000 }

(* let main () =
 *   let v = create "./tzstats-cache" in
 *   let fetch i =
 *     fetch v i >|= fun v ->
 *     ignore v;
 *     Fmt.epr "> lvl:%7d\n%!" i
 *   in
 *   fetch 1513837 >>= fun () ->
 *   fetch 1515607 >>= fun () ->
 *   fetch 1518258 >>= fun () ->
 *   fetch 1518329 >>= fun () ->
 *   let rec aux i = fetch i >>= fun () -> aux (i + request_size) in
 *   aux 0
 *
 * let () =
 *   Lwt_main.run (main ()) |> ignore *)
