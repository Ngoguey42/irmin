open Tezos_context_encoding.Context

module type S = sig
  include Irmin.S

  val sync : repo -> unit

  val clear : repo -> unit Lwt.t
  (* val migrate : Irmin.config -> unit *)
  (* val flush : repo -> unit *)
  (* val reconstruct_index : ?output:string -> Irmin.config -> unit *)

  val create_repo : readonly:bool -> repo Lwt.t
end
with type hash = Hash.t
 and type key = Path.t
 and type step = Path.step
 and type metadata = Metadata.t
 and type contents = Contents.t
 and type branch = Branch.t

module type Sigs = sig
  module Make (Store : S) : Tezos_context_sigs.Context.S
end
