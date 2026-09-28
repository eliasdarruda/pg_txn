defmodule PgTxn.DJSON do
  @moduledoc """
  Canonical durable value encoding, byte-compatible with @pg-txn/client:
  JSON with sorted keys and tagged extensions ($bigint, $bytes, $date).
  Binaries that are valid UTF-8 are strings; use `{:bytes, bin}` for bytes.

  Keys sort by UTF-16 code units like JavaScript's default sort (not by code
  point), so non-BMP keys order identically in every client. Strings (values
  and keys) PostgreSQL jsonb cannot store (U+0000; surrogates are never valid
  UTF-8) raise an ArgumentError naming the path.
  """
  @max_safe 9_007_199_254_740_991
  @tags ["$bigint", "$bytes", "$date", "$undefined", "$escape"]

  def encode!(value), do: value |> to_tagged() |> canonical()

  def decode!(text), do: text |> Jason.decode!() |> from_tagged()

  def to_tagged(value), do: to_tagged(value, "")

  defp to_tagged(nil, _path), do: nil
  defp to_tagged(v, _path) when is_boolean(v), do: v
  defp to_tagged(v, _path) when is_integer(v) and abs(v) <= @max_safe, do: v
  defp to_tagged(v, _path) when is_integer(v), do: %{"$bigint" => Integer.to_string(v)}
  defp to_tagged(v, _path) when is_float(v), do: v
  defp to_tagged(v, path) when is_atom(v), do: v |> Atom.to_string() |> check_string!(path, "string")
  defp to_tagged({:bytes, b}, _path) when is_binary(b), do: %{"$bytes" => Base.encode64(b)}
  defp to_tagged(%DateTime{} = d, _path), do: %{"$date" => d |> DateTime.shift_zone!("Etc/UTC") |> DateTime.truncate(:millisecond) |> iso()}
  defp to_tagged(v, path) when is_binary(v), do: check_string!(v, path, "string")
  defp to_tagged(v, path) when is_list(v) do
    v |> Enum.with_index() |> Enum.map(fn {x, i} -> to_tagged(x, "#{path}[#{i}]") end)
  end
  defp to_tagged(%{} = m, path) when not is_struct(m) do
    out =
      Map.new(m, fn {k, v} ->
        k = to_string(k)
        p = if path == "", do: k, else: "#{path}.#{k}"
        {check_string!(k, p, "object key"), to_tagged(v, p)}
      end)
    case Map.keys(out) do
      [k] when k in @tags -> %{"$escape" => out}
      _ -> out
    end
  end
  defp to_tagged(v, path), do: raise(ArgumentError, "not a durable value: #{inspect(v)} at #{path_text(path)}")

  defp check_string!(s, path, what) do
    cond do
      not String.valid?(s) -> raise(ArgumentError, "binary is not UTF-8 at #{path_text(path)}; wrap bytes as {:bytes, bin}")
      String.contains?(s, <<0>>) -> raise(ArgumentError, "#{what} contains U+0000, which PostgreSQL jsonb cannot store at #{path_text(path)}")
      true -> s
    end
  end

  defp path_text(""), do: "<root>"
  defp path_text(path), do: path

  # big-endian UTF-16 bytes compare like JavaScript's code-unit string order
  defp utf16_order(k), do: :unicode.characters_to_binary(k, :utf8, {:utf16, :big})

  def from_tagged(l) when is_list(l), do: Enum.map(l, &from_tagged/1)
  def from_tagged(%{"$bigint" => s} = m) when map_size(m) == 1, do: String.to_integer(s)
  def from_tagged(%{"$bytes" => s} = m) when map_size(m) == 1, do: {:bytes, Base.decode64!(s)}
  def from_tagged(%{"$date" => s} = m) when map_size(m) == 1 do
    {:ok, d, _} = DateTime.from_iso8601(s)
    d
  end
  def from_tagged(%{"$undefined" => _} = m) when map_size(m) == 1, do: nil
  def from_tagged(%{"$escape" => inner} = m) when map_size(m) == 1, do: Map.new(inner, fn {k, v} -> {k, from_tagged(v)} end)
  def from_tagged(%{} = m), do: Map.new(m, fn {k, v} -> {k, from_tagged(v)} end)
  def from_tagged(v), do: v

  defp iso(d) do
    s = DateTime.to_iso8601(d)
    # always three fractional digits like JavaScript's toISOString
    case Regex.run(~r/^(.*T\d\d:\d\d:\d\d)(\.\d+)?Z$/, s) do
      [_, base] -> base <> ".000Z"
      [_, base, frac] -> base <> String.pad_trailing(String.slice(frac, 0, 4), 4, "0") <> "Z"
    end
  end

  defp canonical(nil), do: "null"
  defp canonical(true), do: "true"
  defp canonical(false), do: "false"
  defp canonical(v) when is_integer(v), do: Integer.to_string(v)
  defp canonical(v) when is_float(v), do: js_number(v)
  defp canonical(v) when is_binary(v), do: js_string(v)
  defp canonical(l) when is_list(l), do: "[" <> Enum.map_join(l, ",", &canonical/1) <> "]"
  defp canonical(%{} = m) do
    "{" <> (m |> Map.keys() |> Enum.sort_by(&utf16_order/1) |> Enum.map_join(",", fn k -> js_string(k) <> ":" <> canonical(m[k]) end)) <> "}"
  end

  # JSON.stringify string escaping (lowercase \u00xx, no "/" escaping)
  defp js_string(s) do
    body =
      for <<c::utf8 <- s>>, into: "" do
        case c do
          ?" -> "\\\""
          ?\\ -> "\\\\"
          ?\b -> "\\b"
          ?\f -> "\\f"
          ?\n -> "\\n"
          ?\r -> "\\r"
          ?\t -> "\\t"
          c when c < 0x20 -> "\\u" <> String.pad_leading(String.downcase(Integer.to_string(c, 16)), 4, "0")
          c -> <<c::utf8>>
        end
      end
    "\"" <> body <> "\""
  end

  defp js_number(f) do
    if f == Float.round(f) and abs(f) <= @max_safe do
      Integer.to_string(trunc(f))
    else
      :erlang.float_to_binary(f, [:short]) |> js_exponent()
    end
  end

  defp js_exponent(s) do
    case String.split(s, "e") do
      [m] -> m
      [m, e] ->
        n = String.to_integer(e)
        if n > -7 and n < 21 do
          Decimal.new(s) |> Decimal.normalize() |> Decimal.to_string(:normal)
        else
          m = String.replace_suffix(m, ".0", "")
          m <> "e" <> if(n > 0, do: "+", else: "-") <> Integer.to_string(abs(n))
        end
    end
  end
end
