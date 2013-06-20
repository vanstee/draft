ExUnit.start

defmodule TestHelper do
  import ExUnit.Assertions

  def assert_receive_event(event) do
    assert_receive { :'$gen_event', ^event }
  end
end
