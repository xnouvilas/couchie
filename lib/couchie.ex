defmodule Couchie do
	@moduledoc """
	Minimalist Elixir interface to Couchbase 2.0.

	Couchie is based on cberl which is a NIF of the libcouchbase & Jiffy JSON encoder NIF.

	JSON support is built in.  Pass in terms and they are encoded as JSON.
	When you  fetch JSON documents you get terms.

	To store raw data, pass in a binary.
	"""

	def url(:domain),
		do: "http://localhost"

	def url(:n1ql),
		do: url(:domain) <> ":8093/query/service"

	def url(:view),
		do: url(:domain) <> ":8092"

	def url(:view, bucket, view),
		do: "#{url(:view)}/#{bucket}/_design/#{view}/_view/#{view}"

	def url(:view, bucket, view, options) when is_list(options) do
		url = "#{url(:view, bucket, view)}"

		params = options
		|> Enum.map(fn{key, value} -> "#{key}=#{value}" end)
		|> Enum.join("&")

		"#{url}?#{params}"
	end

	def url(:view, bucket, view, id) do
		"#{url(:view, bucket, view)}" <>
		"?limit=6&stale=false&connection_timeout=120000&inclusive_end=true" <>
		"&skip=0&full_set=&group=true&key=%22#{id}%22"
	end

	@doc """
	Open a connection pool to the server:
	Open takes a connection configuration consisting of connection name,
	size of the pool to set up, hostname & port, username, password.

	## Examples

			# open connection named "default_connection" to the default bucket, which should be used for testing only
			Couchie.open(:default_connection)
			{ok, <0.XX.0>} #=> successful connection to default bucket on localhost

			# if your bucket is password protected:
			Couchie.open(:secret, 10, 'localhost:8091', 'bucket_name', 'bucket_pasword')
			{ok, <0.XX.0>} #=> successful connection to the named bucket, which you can access using the id "secret"

			# if your bucket isn't password protected (and isn't default)
			Couchie.open(:connection, 10, 'localhost:8091', 'bucket_name')
			{ok, <0.XX.0>} #=> successful connection to the named bucket, which you can access using the id "application"
	"""

	def open(name),
		do:	open(name, 10, 'localhost:8091')

	def open(name, size),
		do:	open(name, size, 'localhost:8091')

	def open(name, size, host),
		do:	open(name, size, host, '', '', '')

	def open(name, size, host, bucket) do
		open(name, size, host, bucket, bucket, bucket)
	end

	def open(name, size, host, bucket, password),  # username is same as bucket name
		do:	open(name, size, host, bucket, bucket, password)

	def open(name, size, host, bucket, username, pass) do  #currently usernames are set to bucket names in this interface.
		IO.puts "Connecting to #{bucket} "
		:cberl.start_link(name, size, host, to_charlist(username), to_charlist(pass), to_charlist(bucket), Couchie.Transcoder)
	end

	@doc """
	Shutdown the connection to a particular bucket

		Couchie.close(:connection)
	"""
	def close(pool),
		do:	:cberl.stop(pool)

	@doc """
	Create document if it doesn't exist, or replace it if it does.
	First parameter is the connection you passed into Couchie.open()

	## Examples

		Couchie.set(:default, "key", "document data")
	"""
	def set(connection, key, document),
		do:	set(connection, key, document, 0)

	@doc """
	Create document if it doesn't exist, or replace it if it does.
	First parameter is the connection you passed into Couchie.open()
	If you want the document to be purged after a period of time, use the Expiration.
	Set expiration to zero for permanent storage (or use set/3)

	## Example

		Couchie.set(:default, "key", "document data", 0)
	"""
	def set(connection, key, document, expiration),
		do:	:cberl.set(connection, key, expiration, document)  # NOTE: cberl parameter order is different!

	def set(connection, key, document, expiration, type),
		do:	:cberl.set(connection, key, expiration, document, type)  # NOTE: cberl parameter order is different!

	def save(element, bucket),
		do: set(bucket, to_string(element[:id]), element)


	@doc """
	Get document.  Keys should be binary.
	## Example

		Couchie.get(:connection, "test_key")
		#=> {"test_key" 1234567890, "value"}  # The middle figure is the CAS for this document.
	"""

  def exists(bucket, id),
    do: get(bucket, id) != nil

	def get(connection, key, decode_type \\ :map),
		do:	mget(connection, [key], decode_type) |> List.first

	@doc """
	Get multiple documents from a list of keys  Keys should be binary.
	## Example

		Couchie.mget(:connection, ["test_key", "another key"])
	"""
	def list(connection, keys, decode_type \\ :map),
		do: mget(connection, keys, decode_type)

	def mget(connection, keys, decode_type \\ :map) do
		:cberl.mget(connection, keys)
		|> Enum.map(fn(result) -> decode(result, decode_type) end)
	end


	def decode({ _id, _query, data}, :map),
		do: Poison.decode!(data, keys: :atoms)

	def decode({ _id, _query, data}, :list),
		do: Couchie.Parser.parse!(data)

	def decode({ _id, _query, data}, :none),
		do: data

	def decode(_result, _decode_type),
		do: nil


	def identification(user_type) do
		select_user = Application.get_env(:couchie, Couchie)[user_type]
		[
			hackney: [basic_auth: {select_user[:user], select_user[:password]}],
			timeout: 120_000,
			recv_timeout: 120_000
		]
	end


  def select(n1ql_query),
    do: select(n1ql_query, :content)

  def select(n1ql_query, :content),
    do: select(n1ql_query, :full) |> Map.get(:results)

  def select(n1ql_query, :full),
    do: query(Poison.encode!(%{statement: n1ql_query}))


  def query(body) do

		headers = %{"Content-Type" => "application/json", "timeout" => 120_000}

    HTTPoison.post(url(:n1ql), body, headers, identification(:select))
		|> query_result
  end


	def view(bucket, view),
		do: query_view(bucket, view)

	def view(bucket, view, id),
		do: view(bucket, view, id, :content)

	def view(bucket, view, id, :content) do
		case view(bucket, view, id, :full) do
			%{rows: []} ->
				[]
			%{rows: [result | _]} ->
				result.value
		end
	end

	def view(bucket, view, id, :full),
		do: query_view(bucket, view, id)


	def query_view(bucket, view),
		do: query_view(bucket, view, nil, url(:view, bucket, view))

	def query_view(bucket, view, id),
		do: query_view(bucket, view, id, url(:view, bucket, view, id))

	def query_view(_bucket, _view, _id, url) do
		headers = %{"Content-Type" => "application/json", "timeout" => 120_000}

		url
		|> HTTPoison.get(headers, identification(:select))
		|> query_result
	end


	def query_result({:ok, %{body: body}}),
		do: Poison.decode!(body, keys: :atoms)

	def query_result({:error, error}),
		do: raise error



	@doc """
	Delete document.  Key should be binary.
	## Example

		Couchie.delete(:connection, "test_key")
	"""
	def delete(connection, key),
		do:	:cberl.remove(connection, key)

	@doc """
	Empty the contents of the specified bucket, deleting all stored data.
	## Example

		Couchie.flush(:connection)
	"""
	def flush(connection),
		do:	:cberl.flush(connection)

	@doc """
	Delete document.  Key should be binary.
	## Example

		Couchie.delete(:connection, "test_key")
	"""
	def query(connection, doc, view, args),
		do:	:cberl.view(connection, doc, view, args)

	defmodule DesignDoc do
		@moduledoc """
		A struct that encapsulates a single view definition.

		It contains the following fields:

			* `:name`   - the view's name
			* `:map`    - the map function as JavaScript code
			* `:reduce` - the reduce function as JavaScript code (optional)
		"""
		defstruct name: nil, map: nil, reduce: nil
	end

	@doc """
	Creates or updates a view.

	Specify the name of the design you want to create or update as `doc_name`.
	The third parameter can be one view definition or a list of them. See DesignDoc struct above.

	## Example
		Couchie.create_view(:db, "my-views", %Couchie.DesignDoc{name: "only_youtube", map: "function(doc, meta) { if (doc.docType == 'youtube') { emit(doc.docType, doc); }}"})
	"""
	def create_view(connection, doc_name, %DesignDoc{} = view),
		do: create_view(connection, doc_name, [view])

	def create_view(connection, doc_name, views) do
		design_doc = {[{
			"views",
				{ views |> Enum.map(&view_as_json(&1)) }
			}]}
		:cberl.set_design_doc(connection, doc_name, design_doc)
	end

	defp view_as_json(view) do
		# convert one view definition to a tuple that can later be converted to json
		{ view.name,
			{ view
				|> Map.take([:map, :reduce])
				|> Enum.filter(fn {_k, v} -> !is_nil(v) end) # only put fields that are not nil
			}
		}
	end

	@doc """
	Delete view.
	## Example

		Couchie.delete_view(:connection, "design-doc-id")
	"""
	def delete_view(connection, doc_name),
		do:	:cberl.remove_design_doc(connection, doc_name)
end
