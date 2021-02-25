type t = { mutable buffer : bytes; mutable position : int }

let create n = { buffer = Bytes.create n; position = 0 }
let unsafe_bytes b = b.buffer
let length b = b.position
let is_empty b = b.position = 0
let clear b = b.position <- 0

let resize b more =
  let old_pos = b.position in
  let old_len = Bytes.length b.buffer in
  let new_len = ref old_len in
  while old_pos + more > !new_len do
    new_len := 2 * !new_len
  done;
  let new_buffer = Bytes.create !new_len in
  Bytes.blit b.buffer 0 new_buffer 0 b.position;
  b.buffer <- new_buffer

let add_substring b s offset len =
  let new_position = b.position + len in
  if new_position > Bytes.length b.buffer then resize b len;
  Bytes.unsafe_blit_string s offset b.buffer b.position len;
  b.position <- new_position

let add_string b s = add_substring b s 0 (String.length s)
