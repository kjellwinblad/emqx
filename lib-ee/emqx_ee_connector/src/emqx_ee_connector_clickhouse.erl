%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_clickhouse).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([roots/0, fields/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2
]).

-export([connect/1]).

-export([
    query/3,
    execute_batch/3
]).

-export([do_get_status/1]).

-define(CLICKHOUSE_HOST_OPTIONS, #{
    default_port => ?CLICKHOUSE_DEFAULT_PORT
}).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

-type url() :: emqx_http_lib:uri_map().
-reflect_type([url/0]).
-typerefl_from_string({url/0, emqx_http_lib, uri_parse}).

-type prepares() :: #{atom() => binary()}.
-type params_tokens() :: #{atom() => list()}.

-type state() ::
    #{
        poolname := atom(),
        prepare_sql := prepares(),
        params_tokens := params_tokens(),
        prepare_statement := eclickhouse:statement()
    }.

%%=====================================================================

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {url,
            sc(
                url(),
                #{
                    required => true,
                    validator => fun
                        (#{query := _Query}) ->
                            {error, "There must be no query in the url"};
                        (_) ->
                            ok
                    end,
                    desc => ?DESC("base_url")
                }
            )}
    ] ++ emqx_connector_schema_lib:relational_db_fields().

%% ===================================================================
callback_mode() -> always_sync.

-spec on_start(binary(), hoconsc:config()) -> {ok, state()} | {error, _}.
on_start(
    InstId,
    #{
        url := URL,
        database := DB,
        username := User,
        pool_size := PoolSize
    } = Config
) ->
    % {Host, Port} = emqx_schema:parse_server(Server, ?CLICKHOUSE_HOST_OPTIONS),
    ?SLOG(info, #{
        msg => "starting_clickhouse_connector",
        connector => InstId,
        config => emqx_misc:redact(Config)
    }),
    % SslOpts =
    %     case maps:get(enable, SSL) of
    %         true ->
    %             [
    %                 {ssl, required},
    %                 {ssl_opts, emqx_tls_lib:to_client_opts(SSL)}
    %             ];
    %         false ->
    %             [{ssl, false}]
    %     end,
    PoolName = emqx_plugin_libs_pool:pool_name(InstId),
    Options = [
        {url, URL},
        {user, User},
        {key, emqx_secret:wrap(maps:get(password, Config, "public"))},
        {database, DB},
        {auto_reconnect, ?AUTO_RECONNECT_INTERVAL},
        {pool_size, PoolSize},
        {pool, PoolName}
    ],
    InitState = #{poolname => PoolName},
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options) of
        ok ->
            {ok, InitState};
        {error, Reason} ->
            ?tp(
                clickhouse_connector_start_failed,
                #{error => Reason}
            ),
            {error, Reason}
    end.

on_stop(InstId, #{poolname := PoolName}) ->
    ?SLOG(info, #{
        msg => "stopping clickouse connector",
        connector => InstId
    }),
    emqx_plugin_libs_pool:stop_pool(PoolName).

on_query(InstId, {TypeOrKey, NameOrSQL}, #{poolname := _PoolName} = State) ->
    on_query(InstId, {TypeOrKey, NameOrSQL, []}, State);
on_query(
    InstId,
    {TypeOrKey, NameOrSQL, Params},
    #{poolname := PoolName} = State
) ->
    ?SLOG(debug, #{
        msg => "clickhouse connector received sql query",
        connector => InstId,
        type => TypeOrKey,
        sql => NameOrSQL,
        state => State
    }),
    Type = clickhouse_query_type(TypeOrKey),
    {NameOrSQL2, Data} = proc_sql_params(TypeOrKey, NameOrSQL, Params, State),
    on_sql_query(InstId, PoolName, Type, NameOrSQL2, Data).

clickhouse_query_type(sql) ->
    query;
clickhouse_query_type(query) ->
    query;
clickhouse_query_type(_) ->
    clickhouse_query_type(query).

on_batch_query(
    InstId,
    BatchReq,
    #{poolname := PoolName, params_tokens := Tokens, prepare_statement := Sts} = State
) ->
    case BatchReq of
        [{Key, _} = Request | _] ->
            BinKey = to_bin(Key),
            case maps:get(BinKey, Tokens, undefined) of
                undefined ->
                    Log = #{
                        connector => InstId,
                        first_request => Request,
                        state => State,
                        msg => "batch prepare not implemented"
                    },
                    ?SLOG(error, Log),
                    {error, batch_prepare_not_implemented};
                TokenList ->
                    {_, Datas} = lists:unzip(BatchReq),
                    Datas2 = [emqx_plugin_libs_rule:proc_sql(TokenList, Data) || Data <- Datas],
                    St = maps:get(BinKey, Sts),
                    {_Column, Results} = on_sql_query(InstId, PoolName, execute_batch, St, Datas2),
                    %% this local function only suits for the result of batch insert
                    TransResult = fun
                        Trans([{ok, Count} | T], Acc) ->
                            Trans(T, Acc + Count);
                        Trans([{error, _} = Error | _], _Acc) ->
                            Error;
                        Trans([], Acc) ->
                            {ok, Acc}
                    end,

                    TransResult(Results, 0)
            end;
        _ ->
            Log = #{
                connector => InstId,
                request => BatchReq,
                state => State,
                msg => "invalid request"
            },
            ?SLOG(error, Log),
            {error, invalid_request}
    end.

proc_sql_params(query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(prepared_query, SQLOrKey, Params, _State) ->
    {SQLOrKey, Params};
proc_sql_params(TypeOrKey, SQLOrData, Params, #{params_tokens := ParamsTokens}) ->
    Key = to_bin(TypeOrKey),
    case maps:get(Key, ParamsTokens, undefined) of
        undefined ->
            {SQLOrData, Params};
        Tokens ->
            {Key, emqx_plugin_libs_rule:proc_sql(Tokens, SQLOrData)}
    end.

on_sql_query(InstId, PoolName, Type, NameOrSQL, Data) ->
    Result = ecpool:pick_and_do(PoolName, {?MODULE, Type, [NameOrSQL, Data]}, no_handover),
    case Result of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "clickhouse connector do sql query failed",
                connector => InstId,
                type => Type,
                sql => NameOrSQL,
                reason => Reason
            });
        _ ->
            ?tp(
                clickhouse_connector_query_return,
                #{result => Result}
            ),
            ok
    end,
    Result.

on_get_status(_InstId, #{poolname := Pool} = _State) ->
    case emqx_plugin_libs_pool:health_check_ecpool_workers(Pool, fun ?MODULE:do_get_status/1) of
        true ->
            connected;
        false ->
            connecting
    end.

do_get_status(Conn) ->
    Status = clickhouse:status(Conn),
    Status.

%% ===================================================================

connect(Opts) ->
    URL = iolist_to_binary(emqx_http_lib:normalize(proplists:get_value(url, Opts))),
    User = proplists:get_value(user, Opts),
    Database = proplists:get_value(database, Opts),
    Key = emqx_secret:unwrap(proplists:get_value(key, Opts)),
    Pool = proplists:get_value(pool, Opts),
    PoolSize = proplists:get_value(pool_size, Opts),
    FixedOptions = [
        {url, URL},
        {database, Database},
        {user, User},
        {key, Key},
        {pool, Pool},
        {pool_size, PoolSize}
    ],
    case clickhouse:start_link(FixedOptions) of
        {ok, _Conn} = Ok ->
            Ok;
        {error, Reason} ->
            {error, Reason}
    end.

% show(Label, What) ->
%     erlang:display({Label, What}),
%     What.

query(Conn, SQL, Params) ->
    clickhouse:query(Conn, SQL, Params).

execute_batch(Conn, Statement, Params) ->
    epgsql:execute_batch(Conn, Statement, Params).

%% Do we need this later for ssl?
%%
% conn_opts(Opts) ->
%     conn_opts(Opts, []).
% conn_opts([], Acc) ->
%     Acc;
% conn_opts([Opt = {database, _} | Opts], Acc) ->
%     conn_opts(Opts, [Opt | Acc]);
% conn_opts([{ssl, Bool} | Opts], Acc) when is_boolean(Bool) ->
%     Flag =
%         case Bool of
%             true -> required;
%             false -> false
%         end,
%     conn_opts(Opts, [{ssl, Flag} | Acc]);
% conn_opts([Opt = {port, _} | Opts], Acc) ->
%     conn_opts(Opts, [Opt | Acc]);
% conn_opts([Opt = {timeout, _} | Opts], Acc) ->
%     conn_opts(Opts, [Opt | Acc]);
% conn_opts([Opt = {ssl_opts, _} | Opts], Acc) ->
%     conn_opts(Opts, [Opt | Acc]);
% conn_opts([_Opt | Opts], Acc) ->
%     conn_opts(Opts, Acc).

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom).
