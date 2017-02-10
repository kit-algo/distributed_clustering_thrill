def chunk_hash(i, num_chunks, n)
  chunk_size = (n + num_chunks - 1) / num_chunks
  chunk = i / chunk_size
  offset = i % chunk_size
  offset * num_chunks + chunk
end

(0...16).map { |i| chunk_hash(i, 4, 16) }
