import { describe, expect, it } from "vitest";
import { wordDiff } from "./diff";

describe("wordDiff", () => {
  it("keeps common words and marks removed and added runs", () => {
    expect(wordDiff("SELECT a FROM t WHERE x = $1", "SELECT a FROM t JOIN u ON u.id = t.id WHERE x = $1")).toEqual([
      { kind: "same", text: "SELECT a FROM t" },
      { kind: "added", text: "JOIN u ON u.id = t.id" },
      { kind: "same", text: "WHERE x = $1" },
    ]);
    expect(wordDiff("a b c", "a c")).toEqual([
      { kind: "same", text: "a" }, { kind: "removed", text: "b" }, { kind: "same", text: "c" },
    ]);
  });

  it("ignores whitespace differences and falls back on very long text", () => {
    expect(wordDiff("SELECT  a\nFROM t", "SELECT a FROM t")).toEqual([{ kind: "same", text: "SELECT a FROM t" }]);
    const long = Array.from({ length: 1600 }, (_, i) => `w${i}`).join(" ");
    expect(wordDiff(long, "x").map((p) => p.kind)).toEqual(["removed", "added"]);
  });
});
