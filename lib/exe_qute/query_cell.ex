defmodule ExeQute.QueryCell do
  @moduledoc false

  use Kino.JS
  use Kino.JS.Live
  use Kino.SmartCell, name: "KDB+ Query"

  @impl true
  def init(attrs, ctx) do
    fields = %{
      "variable" => attrs["variable"] || "result",
      "connection" => attrs["connection"] || "",
      "namespace" => attrs["namespace"] || ".",
      "query" => attrs["query"] || "",
      "limit" => attrs["limit"] || "1000"
    }

    {:ok, assign(ctx, fields: fields, connections: [], namespaces: [], tables: [], functions: [])}
  end

  @impl true
  def handle_connect(ctx) do
    payload = %{
      fields: ctx.assigns.fields,
      connections: ctx.assigns.connections,
      namespaces: ctx.assigns.namespaces,
      tables: ctx.assigns.tables,
      functions: ctx.assigns.functions
    }

    {:ok, payload, ctx}
  end

  @impl true
  def scan_binding(server, binding, _env) do
    conn_map =
      for {name, val} <- binding,
          is_atom(name),
          connection_value?(val),
          into: %{},
          do: {Atom.to_string(name), val}

    send(server, {:connections, conn_map})
  end

  defp connection_value?(val) when is_pid(val), do: true

  defp connection_value?(val) when is_atom(val) do
    val != nil and val != true and val != false and
      Process.whereis(val) != nil
  end

  defp connection_value?(_), do: false

  @impl true
  def handle_info({:connections, conn_map}, ctx) do
    conns = Map.keys(conn_map)
    ctx = assign(ctx, connections: conns, conn_map: conn_map)
    broadcast_event(ctx, "update_connections", %{"connections" => conns})
    {:noreply, ctx}
  end

  @impl true
  def handle_event("update_field", %{"field" => field, "value" => value}, ctx) do
    fields = Map.put(ctx.assigns.fields, field, value)
    {:noreply, assign(ctx, fields: fields)}
  end

  @impl true
  def handle_event("fetch_browser", %{"connection" => conn_name, "namespace" => ns}, ctx) do
    conn = ctx.assigns |> Map.get(:conn_map, %{}) |> Map.get(conn_name)

    if conn do
      ns_arg = if ns == "." or ns == "", do: nil, else: ns
      raw_namespaces = ok_or(ExeQute.namespaces(conn), [])
      namespaces = ["."] ++ raw_namespaces

      tables = ok_or(ExeQute.tables(conn, ns_arg), [])
      functions = ok_or(ExeQute.functions(conn, ns_arg), [])
      function_names = Enum.map(functions, & &1["name"])

      ctx = assign(ctx, namespaces: namespaces, tables: tables, functions: function_names)

      broadcast_event(ctx, "update_namespaces", %{"namespaces" => namespaces})
      broadcast_event(ctx, "update_browser", %{"tables" => tables, "functions" => function_names})

      {:noreply, ctx}
    else
      {:noreply, ctx}
    end
  end

  @impl true
  def handle_event("fetch_browser", %{"connection" => conn_name}, ctx) do
    ns = ctx.assigns.fields["namespace"] || "."
    handle_event("fetch_browser", %{"connection" => conn_name, "namespace" => ns}, ctx)
  end

  @impl true
  def to_attrs(ctx), do: ctx.assigns.fields

  @impl true
  def to_source(%{"variable" => var, "connection" => conn, "query" => query}) do
    q = inspect(query)
    """
    {:ok, #{var}} = ExeQute.query(#{conn}, #{q})
    ExeQute.display(#{var}, #{q})
    """
  end

  defp ok_or({:ok, val}, _), do: val
  defp ok_or(_, default), do: default

  asset "main.js" do
    """
    export function init(ctx, payload) {
      ctx.importCSS("main.css");

      let { fields, connections, namespaces, tables, functions } = payload;

      ctx.root.innerHTML = `
        <div class="qcell">
          <div class="top-row">
            <label class="field">
              <span>Name</span>
              <input type="text" id="variable" value="${esc(fields.variable)}" />
            </label>
            <label class="field">
              <span>Connection</span>
              <select id="connection">${selectOptions(connections, fields.connection)}</select>
            </label>
            <label class="field">
              <span>Namespace</span>
              <select id="namespace">${selectOptions(namespaces, fields.namespace)}</select>
            </label>
            <label class="field narrow">
              <span>Row limit</span>
              <input type="number" id="limit" value="${esc(fields.limit)}" min="0" step="1000" />
            </label>
          </div>
          <div class="main-row">
            <div class="sidebar">
              <div class="tabs">
                <button class="tab active" data-tab="tables">Tables</button>
                <button class="tab" data-tab="functions">Functions</button>
              </div>
              <ul id="browser-list">${browserItems(tables)}</ul>
            </div>
            <div class="editor-col">
              <label class="field full">
                <span>Q expression</span>
                <textarea id="query" rows="5">${esc(fields.query)}</textarea>
              </label>
            </div>
          </div>
        </div>
      `;

      let activeTab = "tables";

      function selectOptions(list, selected) {
        return list.map(c =>
          `<option value="${esc(c)}" ${c === selected ? "selected" : ""}>${esc(c)}</option>`
        ).join("");
      }

      function browserItems(items) {
        return items.map(t =>
          `<li class="browser-item" title="${esc(t)}">${esc(t)}</li>`
        ).join("");
      }

      function push(field, value) {
        ctx.pushEvent("update_field", { field, value });
      }

      function fetchBrowser() {
        const conn = connSelect.value;
        const ns = nsSelect.value;
        if (conn) ctx.pushEvent("fetch_browser", { connection: conn, namespace: ns });
      }

      const connSelect = ctx.root.querySelector("#connection");
      const nsSelect = ctx.root.querySelector("#namespace");
      const varInput = ctx.root.querySelector("#variable");
      const queryArea = ctx.root.querySelector("#query");
      const limitInput = ctx.root.querySelector("#limit");
      const browserList = ctx.root.querySelector("#browser-list");

      varInput.addEventListener("change", e => push("variable", e.target.value));
      queryArea.addEventListener("change", e => push("query", e.target.value));
      limitInput.addEventListener("change", e => push("limit", e.target.value));

      connSelect.addEventListener("change", e => {
        push("connection", e.target.value);
        fetchBrowser();
      });

      nsSelect.addEventListener("change", e => {
        push("namespace", e.target.value);
        fetchBrowser();
      });

      for (const tab of ctx.root.querySelectorAll(".tab")) {
        tab.addEventListener("click", () => {
          ctx.root.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
          tab.classList.add("active");
          activeTab = tab.dataset.tab;
          browserList.innerHTML = browserItems(activeTab === "tables" ? tables : functions);
        });
      }

      browserList.addEventListener("click", e => {
        const item = e.target.closest(".browser-item");
        if (!item) return;
        const name = item.textContent.trim();
        const limit = parseInt(limitInput.value, 10);
        let q;
        if (activeTab === "tables") {
          q = limit > 0 ? `select[${limit}] from ${name}` : `select from ${name}`;
        } else {
          q = `${name}[]`;
        }
        queryArea.value = q;
        push("query", q);
      });

      ctx.handleEvent("update_connections", ({ connections: list }) => {
        connections = list;
        const prev = connSelect.value;
        connSelect.innerHTML = selectOptions(list, prev);
        const selected = connSelect.value || (list.length > 0 ? list[0] : null);
        if (selected) {
          connSelect.value = selected;
          push("connection", selected);
          fetchBrowser();
        }
      });

      ctx.handleEvent("update_namespaces", ({ namespaces: list }) => {
        namespaces = list;
        const prev = nsSelect.value;
        nsSelect.innerHTML = selectOptions(list, prev);
        if (!nsSelect.value && list.length > 0) nsSelect.value = list[0];
      });

      ctx.handleEvent("update_browser", ({ tables: t, functions: f }) => {
        tables = t;
        functions = f;
        browserList.innerHTML = browserItems(activeTab === "tables" ? tables : functions);
      });

      ctx.handleSync(() => {
        push("variable", varInput.value);
        push("query", queryArea.value);
        push("connection", connSelect.value);
        push("namespace", nsSelect.value);
        push("limit", limitInput.value);
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
    .qcell { font-family: sans-serif; font-size: 13px; }
    .top-row { display: flex; gap: 12px; margin-bottom: 10px; flex-wrap: wrap; }
    .field { display: flex; flex-direction: column; gap: 3px; flex: 1; min-width: 120px; }
    .field.narrow { max-width: 130px; flex: 0 0 130px; }
    .field.full { flex: 1; display: flex; flex-direction: column; height: 100%; }
    .field span { font-weight: 600; color: #555; }
    .field input, .field select, .field textarea {
      border: 1px solid #ccc; border-radius: 4px; padding: 4px 8px;
      font-size: 13px; width: 100%; box-sizing: border-box;
    }
    .field textarea { resize: vertical; flex: 1; font-family: monospace; min-height: 100px; }
    .main-row { display: flex; gap: 10px; }
    .sidebar { width: 180px; flex-shrink: 0; border: 1px solid #ddd; border-radius: 4px; overflow: hidden; }
    .tabs { display: flex; border-bottom: 1px solid #ddd; }
    .tab { flex: 1; border: none; background: #f5f5f5; padding: 5px; cursor: pointer; font-size: 12px; }
    .tab.active { background: #fff; font-weight: 600; border-bottom: 2px solid #4a90d9; }
    #browser-list { list-style: none; margin: 0; padding: 0; max-height: 180px; overflow-y: auto; }
    .browser-item { padding: 4px 8px; cursor: pointer; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 12px; }
    .browser-item:hover { background: #e8f0fe; }
    .editor-col { flex: 1; display: flex; flex-direction: column; }
    """
  end
end
