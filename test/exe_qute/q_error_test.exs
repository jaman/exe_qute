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

  @sample_with_message ~S"""
  ** Backtrace: hop. OS reports: Connection refused
    [4]  h:hopen `:204.8.241.101:11202:rdb:pass; r:h"(value;`.support.status_state)"; hclose h; r
           ^
    [3]  /opt/xtp/q/common/initreg.q:162: .qapp.processMsg@:
                  v:value x;
                    ^
    [1]  (.Q.trp)

    [0]  /opt/xtp/q/common/initreg.q:41: .z.pg:{...}
                                                ^
  """

  describe "parse/1" do
    test "returns :error for non-backtrace input" do
      assert :error = QError.parse("nope")
      assert :error = QError.parse("")
    end

    test "parses every frame from the sample backtrace" do
      assert {:ok, %QError{frames: frames, raw: raw, message: nil}} = QError.parse(@sample)
      assert raw == @sample
      assert Enum.map(frames, & &1.level) == [5, 4, 3, 2, 1, 0]
    end

    test "extracts inline error message when the gateway includes one" do
      assert {:ok, %QError{message: msg, frames: frames}} =
               QError.parse(@sample_with_message)

      assert msg == "hop. OS reports: Connection refused"
      assert Enum.map(frames, & &1.level) == [4, 3, 1, 0]

      frame_4 = Enum.find(frames, &(&1.level == 4))
      assert String.starts_with?(frame_4.code, "h:hopen")
    end

    test "leaves message nil when the first non-prefix content is a frame marker" do
      assert {:ok, %QError{message: nil}} = QError.parse(@sample)
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

  describe "from_response/1" do
    test "converts {:ok, backtrace_string} to {:error, %QError{}}" do
      assert {:error, %QError{}} = QError.from_response({:ok, @sample})
    end

    test "converts {:error, backtrace_string} to {:error, %QError{}}" do
      assert {:error, %QError{}} = QError.from_response({:error, @sample})
    end

    test "extracts the inline message when present on a raised backtrace" do
      assert {:error, %QError{message: "hop. OS reports: Connection refused"}} =
               QError.from_response({:error, @sample_with_message})
    end

    test "passes through non-backtrace :ok results unchanged" do
      assert {:ok, [1, 2, 3]} = QError.from_response({:ok, [1, 2, 3]})
      assert {:ok, "regular string"} = QError.from_response({:ok, "regular string"})
    end

    test "passes through non-backtrace :error results unchanged" do
      assert {:error, :timeout} = QError.from_response({:error, :timeout})
      assert {:error, "type"} = QError.from_response({:error, "type"})
      assert {:error, {:connection_error, _}} =
               QError.from_response({:error, {:connection_error, :econnrefused}})
    end

    test "passes through unrecognised shapes unchanged" do
      assert :ok = QError.from_response(:ok)
      assert nil == QError.from_response(nil)
    end
  end

  describe "trap/1" do
    test "wraps a plain query in the @[...] trap form" do
      assert QError.trap("select from trade") ==
               ~s|@[{(`exe_qute_trap_ok;value x)};"select from trade";{(`exe_qute_trap_err;x)}]|
    end

    test "escapes embedded double quotes" do
      wrapped = QError.trap(~s|h"value `.a"|)
      assert wrapped ==
               ~s|@[{(`exe_qute_trap_ok;value x)};"h\\"value `.a\\"";{(`exe_qute_trap_err;x)}]|
    end

    test "escapes embedded backslashes" do
      wrapped = QError.trap(~S|a\b|)
      assert wrapped ==
               ~S|@[{(`exe_qute_trap_ok;value x)};"a\\b";{(`exe_qute_trap_err;x)}]|
    end
  end

  describe "untrap/1" do
    test "unwraps a tagged success" do
      assert {:ok, [1, 2, 3]} = QError.untrap({:ok, ["exe_qute_trap_ok", [1, 2, 3]]})
    end

    test "unwraps a tagged error to {:error, message}" do
      assert {:error, "hop. OS reports: Connection refused"} =
               QError.untrap({:ok, ["exe_qute_trap_err", "hop. OS reports: Connection refused"]})
    end

    test "passes through results without a trap tag" do
      assert {:ok, [1, 2, 3]} = QError.untrap({:ok, [1, 2, 3]})
      assert {:ok, "plain"} = QError.untrap({:ok, "plain"})
      assert {:error, :timeout} = QError.untrap({:error, :timeout})
    end
  end
end
