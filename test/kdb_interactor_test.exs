defmodule KdbInteractorTest do
  use ExUnit.Case
  doctest KdbInteractor

  test "greets the world" do
    assert KdbInteractor.hello() == :world
  end
end
