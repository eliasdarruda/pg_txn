defmodule PgTxn.DJSONTest do
  # pure encoding tests: no database needed
  use ExUnit.Case, async: true
  alias PgTxn.DJSON

  @vectors System.get_env("DJSON_VECTORS", "/work/clients/djson-vectors.json")

  defp decode_spec(%{"t" => "json", "v" => v}), do: v
  defp decode_spec(%{"t" => "bigint", "v" => v}), do: String.to_integer(v)
  defp decode_spec(%{"t" => "bytes", "v" => v}), do: {:bytes, :binary.list_to_bin(v)}
  defp decode_spec(%{"t" => "date", "v" => v}), do: elem(DateTime.from_iso8601(v), 1)
  defp decode_spec(%{"t" => "float", "v" => v}), do: elem(Float.parse(v), 0)
  defp decode_spec(%{"t" => "list", "v" => v}), do: Enum.map(v, &decode_spec/1)
  defp decode_spec(%{"t" => "obj", "v" => v}), do: Map.new(v, fn {k, x} -> {k, decode_spec(x)} end)

  test "keys sort by UTF-16 code units like JavaScript" do
    # U+1F600 is D83D DE00 in UTF-16 and sorts before U+E000 and U+FB01
    assert DJSON.encode!(%{"ﬁ" => 1, "\u{1F600}" => 2, "" => 3}) == ~s({"\u{1F600}":2,"":3,"ﬁ":1})
    assert DJSON.encode!(%{"￿" => 1, "\u{10000}" => 2}) == ~s({"\u{10000}":2,"￿":1})
  end

  test "strings PostgreSQL jsonb cannot store are rejected with a path" do
    assert_raise ArgumentError, ~r/string contains U\+0000.* at <root>/, fn -> DJSON.encode!("a\0b") end
    assert_raise ArgumentError, ~r/string contains U\+0000.* at a\[1\]/, fn -> DJSON.encode!(%{"a" => ["ok", <<0>>]}) end
    assert_raise ArgumentError, ~r/object key contains U\+0000.* at k/, fn -> DJSON.encode!(%{"k\0" => 1}) end
    # an encoded surrogate (CESU-8 style) is not valid UTF-8
    assert_raise ArgumentError, ~r/not UTF-8 at s/, fn -> DJSON.encode!(%{"s" => <<0xED, 0xA0, 0x80>>}) end
    assert DJSON.encode!("\x01") == ~s("\\u0001")
  end

  # shared with the other clients
  test "shared rejection vectors" do
    vectors = @vectors |> File.read!() |> Jason.decode!()
    errors = for %{"value" => spec, "error" => msg} <- vectors, do: {spec, msg}
    assert errors != []
    for {spec, msg} <- errors do
      assert_raise ArgumentError, ~r/#{Regex.escape(msg)}/, fn -> DJSON.encode!(decode_spec(spec)) end
    end
  end

  test "byte-compatible with the TypeScript client" do
    vectors = @vectors |> File.read!() |> Jason.decode!()

    for %{"value" => spec, "expect" => expect} <- vectors do
      value = decode_spec(spec)
      small_bigint = spec["t"] == "bigint" and abs(value) <= 9_007_199_254_740_991
      unless small_bigint, do: assert(DJSON.encode!(value) == expect, inspect(spec))
      assert DJSON.encode!(DJSON.decode!(expect)) == if(small_bigint, do: Integer.to_string(value), else: expect)
    end
  end
end
