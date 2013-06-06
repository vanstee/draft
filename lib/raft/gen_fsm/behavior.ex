defmodule GenFsm.Behaviour do
  defmacro __using__(_) do
    quote location: :keep do
      @behavior :gen_fsm

      def init(args) do
        { :ok, args }
      end

      def handle_event(_event, state_name, state_data) do
        { :next_state, state_name, state_data }
      end

      def handle_sync_event(_event, _from, state_name, state_data) do
        { :next_state, state_name, state_data }
      end

      def handle_info(_info, state_name, state_data) do
        { :next_state, state_name, state_data }
      end

      def terminate(_reason, _state_name, _state_data) do
        :ok
      end

      def code_change(_old_vsn, state_name, state_data, _extra) do
        { :ok, state_name, state_data }
      end

      defoverridable [init: 1, handle_event: 3, handle_sync_event: 4,
        handle_info: 3, terminate: 3, code_change: 4]
    end
  end
end
