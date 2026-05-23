defmodule ExeQute.QErrorTest do
  use ExUnit.Case, async: true

  alias ExeQute.QError

  @sample ~S"""
  ** Backtrace:  [5]  {.fe.h:hopen(`:204.8.241.101:11202:rdb:pass;1000); res:@[{.fe.h(x)};(value;`.support.status_state);{enlist `name`time`status`info!(`;.z.P;1;"")}]; hclose .fe.h; :res}
                ^
    [4]  {.fe.h:hopen(`:204.8.241.101:11202:rdb:pass;1000); res:@[{.fe.h(x)};(value;`.support.status_state);{enlist `name`time`status`info!(`;.z.P;1;"")}]; hclose .fe.h; :res}[]
         ^
    [3]  /opt/xtp/q/common/initreg.q:162: .qapp.processMsg@:
                      ];
                  v:value x;
                    ^
                  if[or[.z.w;.qapp.replay_count]&long_query_timeout<(now:.z.P)-start;
    [2]  /opt/xtp/q/common/initreg.q:150: .qapp.processMsg:
                  ];
              {
              ^
                  start:.z.P;
    [1]  (.Q.trp)

    [0]  /opt/xtp/q/common/initreg.q:41: .z.pg:{$[value "\\e";.qapp.processMsg[0b;x];.Q.trp[.qapp.processMsg[0b;];x;{[e;bt;a] .app_rep.prn "Error [",e,"] processing msg ",.Q.s1 a;0N!"** Backtrace:",.Q.sbt bt}[;;x]]]}
                                                                                     ^
  """

  describe "backtrace?/1" do
    test "true for strings starting with the backtrace marker" do
      assert QError.backtrace?(@sample)
      assert QError.backtrace?("** Backtrace:")
    end

    test "false for other strings and non-strings" do
      refute QError.backtrace?("ok")
      refute QError.backtrace?("")
      refute QError.backtrace?(nil)
      refute QError.backtrace?(%{})
    end
  end

  describe "parse/1" do
    test "returns :error for non-backtrace input" do
      assert :error = QError.parse("nope")
      assert :error = QError.parse("")
    end

    test "parses every frame from the sample backtrace" do
      assert {:ok, %QError{frames: frames, raw: raw}} = QError.parse(@sample)
      assert raw == @sample
      assert Enum.map(frames, & &1.level) == [5, 4, 3, 2, 1, 0]
    end

    test "frame without file/function captures code only" do
      assert {:ok, %QError{frames: frames}} = QError.parse(@sample)
      frame_5 = Enum.find(frames, &(&1.level == 5))
      assert frame_5.file == nil
      assert frame_5.line == nil
      assert frame_5.function == nil
      assert String.starts_with?(frame_5.code, "{.fe.h:hopen")
    end

    test "frame with file:line:function captures all parts" do
      assert {:ok, %QError{frames: frames}} = QError.parse(@sample)
      frame_3 = Enum.find(frames, &(&1.level == 3))
      assert frame_3.file == "/opt/xtp/q/common/initreg.q"
      assert frame_3.line == 162
      assert frame_3.function == ".qapp.processMsg@"
      assert String.contains?(frame_3.code, "v:value x;")
    end

    test "special (.Q.trp) frame has no file/function" do
      assert {:ok, %QError{frames: frames}} = QError.parse(@sample)
      frame_1 = Enum.find(frames, &(&1.level == 1))
      assert frame_1.file == nil
      assert frame_1.line == nil
      assert frame_1.function == nil
      assert frame_1.code == "(.Q.trp)"
    end

    test "frame with inline body keeps the body in code" do
      assert {:ok, %QError{frames: frames}} = QError.parse(@sample)
      frame_0 = Enum.find(frames, &(&1.level == 0))
      assert frame_0.file == "/opt/xtp/q/common/initreg.q"
      assert frame_0.line == 41
      assert frame_0.function == ".z.pg"
      assert String.starts_with?(frame_0.code, "{$[value")
    end
  end

  describe "Inspect" do
    test "renders the raw backtrace inside the wrapper" do
      {:ok, qerror} = QError.parse(@sample)
      rendered = inspect(qerror)
      assert String.starts_with?(rendered, "#ExeQute.QError<")
      assert String.ends_with?(rendered, ">")
      assert String.contains?(rendered, "** Backtrace:")
      assert String.contains?(rendered, ".qapp.processMsg")
    end
  end

  describe "String.Chars" do
    test "to_string/1 returns the raw backtrace" do
      {:ok, qerror} = QError.parse(@sample)
      assert to_string(qerror) == @sample
    end
  end
end
