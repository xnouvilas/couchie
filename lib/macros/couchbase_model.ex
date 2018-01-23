defmodule Couchie.Macros.CouchbaseModel do

  defmacro couchbase_find(model_table) do

    table = case Application.get_env(:couchie, Couchie)[:buckets] do
      ["tests"] ->
        :tests
      _ ->
        model_table
    end

    quote do

      def get(id, type \\ :map),
        do: Couchie.get(unquote(table), to_string(id), type)

      def set(id, data),
        do: Couchie.set(unquote(table), to_string(id), data)

      def delete(id),
        do: Couchie.delete(unquote(table), to_string(id))


      def last,
        do: find()

      def find,
        do: query_one("#{query_base()} LIMIT 1")

      def find(where) when is_list(where),
        do: query_one("#{query_base()} WHERE #{arguments(where)} LIMIT 1")

      def find(id),
        do: find(id: id)


      def where(where),
        do: list(where)

      def list,
        do: query_list("#{query_base()}")

      def list(where),
        do: query_list("#{query_base()} WHERE #{arguments(where)}")

      def list([], params),
        do: query_list("#{query_base()} #{refinements(params)}")

      def list(where, params),
        do: query_list("#{query_base()} WHERE #{arguments(where)} #{refinements(params)}")


      def count,
        do: query_one("#{query_base(:count)}")

      def count(where),
        do: query_one("#{query_base(:count)} WHERE #{arguments(where)}")


      def query_base,
        do: "SELECT #{unquote(table)}.* FROM #{unquote(table)}"

      def query_base(:count),
        do: "SELECT RAW COUNT(*) as count FROM #{unquote(table)}"


      def query_one(n1ql_query),
        do: query_list(n1ql_query) |> List.first

      def query_list(n1ql_query),
        do: Couchie.select(n1ql_query)


      def arguments(where) do
        where
        |> Enum.map(fn{field, value} -> argument(field, value) end)
        |> Enum.join(" AND ")
      end


      def argument(field, :is_null),
        do: "#{to_string(field)} IS NULL"

      def argument(field, :is_not_null),
        do: "#{to_string(field)} IS NOT NULL"

      def argument(field, value) when is_bitstring(value),
        do: "#{to_string(field)} = '#{to_string(value)}'"

      def argument(field, value),
        do: "#{to_string(field)} = #{to_string(value)}"


      def refinements(params) do
        params
        |> Enum.map(fn{action, value} -> action(action, value) end)
        |> Enum.join(" ")
      end

      def action(:limit, value),
        do: "LIMIT #{value}"

      def action(:order, value),
        do: "ORDER BY #{value}"

      def action(:where, value),
        do: "WHERE #{value}"


      def struct_from_map(list, as: struct) when is_list(list) do
        Enum.map(list, fn elem ->
          struct_from_map(elem, as: struct)
        end)
      end

      def struct_from_map(map, as: struct) do
        # Find the keys within the map
        keys = Map.keys(struct)
        |> Enum.filter(fn x -> x != :__struct__ end)

        # Process map, checking for both string / atom keys
        processed_map =
         for key <- keys, into: %{} do
           value = Map.get(map, key) || Map.get(map, to_string(key))
           {key, value}
         end

        Map.merge(struct, processed_map)
      end

    end

  end

end
