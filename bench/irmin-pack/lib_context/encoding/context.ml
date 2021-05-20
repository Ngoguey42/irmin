(*****************************************************************************)
(*                                                                           *)
(* Open Source License                                                       *)
(* Copyright (c) 2018-2021 Tarides <contact@tarides.com>                     *)
(*                                                                           *)
(* Permission is hereby granted, free of charge, to any person obtaining a   *)
(* copy of this software and associated documentation files (the "Software"),*)
(* to deal in the Software without restriction, including without limitation *)
(* the rights to use, copy, modify, merge, publish, distribute, sublicense,  *)
(* and/or sell copies of the Software, and to permit persons to whom the     *)
(* Software is furnished to do so, subject to the following conditions:      *)
(*                                                                           *)
(* The above copyright notice and this permission notice shall be included   *)
(* in all copies or substantial portions of the Software.                    *)
(*                                                                           *)
(* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR*)
(* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,  *)
(* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL   *)
(* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER*)
(* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING   *)
(* FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER       *)
(* DEALINGS IN THE SOFTWARE.                                                 *)
(*                                                                           *)
(*****************************************************************************)

module Hash : sig
  include Irmin.Hash.S

  val to_context_hash : t -> Context_hash.t

  val of_context_hash : Context_hash.t -> t
end = struct
  module H = Digestif.Make_BLAKE2B (struct
    let digest_size = 32
  end)

  type t = H.t

  let of_context_hash s = H.of_raw_string (Context_hash.to_string s)

  let to_context_hash h = Context_hash.of_string_exn (H.to_raw_string h)

  let pp ppf t = Context_hash.pp ppf (to_context_hash t)

  let of_string x =
    match Context_hash.of_b58check x with
    | Ok x -> Ok (of_context_hash x)
    | Error err ->
        Error
          (`Msg
            (Format.asprintf
               "Failed to read b58check_encoding data: %a"
               Error_monad.pp_print_error
               err))

  let short_hash_string = Irmin.Type.(unstage (short_hash string))

  let short_hash_staged =
    Irmin.Type.stage @@ fun ?seed t ->
    short_hash_string ?seed (H.to_raw_string t)

  let t : t Irmin.Type.t =
    Irmin.Type.map
      ~pp
      ~of_string
      Irmin.Type.(string_of (`Fixed H.digest_size))
      ~short_hash:short_hash_staged
      H.of_raw_string
      H.to_raw_string

  let short_hash =
    let f = short_hash_string ?seed:None in
    fun t -> f (H.to_raw_string t)

  let hash_size = H.digest_size

  let hash = H.digesti_string
end

module Node = Tezos_context_hash_irmin.Encoding.Node
module Commit = Tezos_context_hash_irmin.Encoding.Commit
module Contents = Tezos_context_hash_irmin.Encoding.Contents
module Path = Tezos_context_hash_irmin.Encoding.Path
module Metadata = Tezos_context_hash_irmin.Encoding.Metadata
module Branch = Tezos_context_hash_irmin.Encoding.Branch
