defmodule ExeQute.ConnectionCell do
  @moduledoc false

  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "KDB+ Connection"

  @impl true
  def init(attrs, ctx) do
    fields = %{
      "variable" => attrs["variable"] || "conn",
      "host" => attrs["host"] || "localhost",
      "port" => attrs["port"] || "5001",
      "username" => attrs["username"] || "",
      "secret" => attrs["secret"] || ""
    }

    {:ok, assign(ctx, fields: fields)}
  end

  @impl true
  def handle_connect(ctx) do
    {:ok, %{fields: ctx.assigns.fields}, ctx}
  end

  @impl true
  def handle_event("update_field", %{"field" => field, "value" => value}, ctx) do
    fields = Map.put(ctx.assigns.fields, field, value)
    {:noreply, assign(ctx, fields: fields)}
  end

  @impl true
  def to_attrs(ctx), do: ctx.assigns.fields

  @impl true
  def to_source(attrs) do
    %{
      "variable" => var,
      "host" => host,
      "port" => port,
      "username" => username,
      "secret" => secret
    } = attrs

    opts = [~s|host: "#{host}"|, "port: #{port}"]
    opts = if username != "", do: opts ++ [~s|username: "#{username}"|], else: opts

    opts =
      if secret != "",
        do: opts ++ [~s|password: System.fetch_env!("LB_#{secret}")|],
        else: opts

    "{:ok, #{var}} = ExeQute.connect(#{Enum.join(opts, ", ")})"
  end

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      const fields = payload.fields;

      ctx.root.innerHTML = `
        <div class="cell">
          <div class="row">
            <label class="field">
              <span>Variable</span>
              <input type="text" data-field="variable" value="${esc(fields.variable)}" />
            </label>
            <label class="field">
              <span>Host</span>
              <input type="text" data-field="host" value="${esc(fields.host)}" />
            </label>
            <label class="field narrow">
              <span>Port</span>
              <input type="text" data-field="port" value="${esc(fields.port)}" />
            </label>
          </div>
          <div class="row">
            <label class="field">
              <span>Username</span>
              <input type="text" data-field="username" value="${esc(fields.username)}" />
            </label>
            <label class="field">
              <span>Password secret</span>
              <div class="secret-row">
                <input type="text" data-field="secret" value="${esc(fields.secret)}" placeholder="LB_KDB_PASS (without LB_ prefix)" />
                <button class="secret-btn" title="Choose from Livebook secrets">🔑</button>
              </div>
            </label>
          </div>
        </div>
      `;

      function esc(s) {
        return String(s ?? "")
          .replace(/&/g, "&amp;")
          .replace(/"/g, "&quot;")
          .replace(/</g, "&lt;");
      }

      for (const input of ctx.root.querySelectorAll("input[data-field]")) {
        input.addEventListener("change", (e) => {
          ctx.pushEvent("update_field", { field: e.target.dataset.field, value: e.target.value });
        });
      }

      ctx.root.querySelector(".secret-btn").addEventListener("click", () => {
        const input = ctx.root.querySelector("[data-field='secret']");
        ctx.selectSecret((name) => {
          input.value = name;
          ctx.pushEvent("update_field", { field: "secret", value: name });
        }, input.value);
      });

      ctx.handleSync(() => {
        for (const input of ctx.root.querySelectorAll("input[data-field]")) {
          input.dispatchEvent(new Event("change"));
        }
      });
    }

    function esc(s) {
      return String(s ?? "")
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/</g, "&lt;");
    }
    """
  end

  asset "main.css" do
    """
    .cell { font-family: sans-serif; font-size: 13px; padding: 8px 0; }
    .row { display: flex; gap: 12px; margin-bottom: 8px; flex-wrap: wrap; }
    .field { display: flex; flex-direction: column; gap: 3px; flex: 1; min-width: 120px; }
    .field.narrow { max-width: 100px; flex: 0 0 100px; }
    .field span { font-weight: 600; color: #555; }
    .field input { border: 1px solid #ccc; border-radius: 4px; padding: 4px 8px; font-size: 13px; width: 100%; box-sizing: border-box; }
    .secret-row { display: flex; gap: 4px; }
    .secret-row input { flex: 1; }
    .secret-btn { border: 1px solid #ccc; border-radius: 4px; background: #f5f5f5; cursor: pointer; padding: 4px 8px; }
    .secret-btn:hover { background: #e8e8e8; }
    """
  end
end
