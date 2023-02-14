%%--------------------------------------------------------------------
% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% SQL definitions
-define(SQL_BRIDGE,
    "INSERT INTO mqtt_test(payload, arrived) "
    "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
).
-define(SQL_CREATE_TABLE,
    "CREATE TABLE IF NOT EXISTS mqtt_test (payload blob, arrived datetime NOT NULL) "
    "DEFAULT CHARSET=utf8MB4;"
).
-define(SQL_DROP_TABLE, "DROP TABLE mqtt_test").
-define(SQL_DELETE, "DELETE from mqtt_test").
-define(SQL_SELECT, "SELECT payload FROM mqtt_test").

% DB defaults
-define(MYSQL_DATABASE, "mqtt").
-define(MYSQL_USERNAME, "root").
-define(MYSQL_PASSWORD, "public").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    NonBatchCases = [t_write_timeout, t_uninitialized_prepared_statement],
    [
        {tcp, [
            {group, with_batch},
            {group, without_batch}
        ]},
        {tls, [
            {group, with_batch},
            {group, without_batch}
        ]},
        {with_batch, TCs -- NonBatchCases},
        {without_batch, TCs}
    ].

init_per_group(tcp, Config) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, false},
        {query_mode, sync},
        {proxy_name, "mysql_tcp"}
        | Config
    ];
init_per_group(tls, Config) ->
    MysqlHost = os:getenv("MYSQL_TLS_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TLS_PORT", "3307")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, true},
        {query_mode, sync},
        {proxy_name, "mysql_tls"}
        | Config
    ];
init_per_group(with_batch, Config0) ->
    Config = [{batch_size, 100} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    connect_and_drop_table(Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    connect_and_clear_table(Config),
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    connect_and_clear_table(Config),
    ok = snabbkaffe:stop(),
    delete_bridge(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(Config0) ->
    BridgeType = <<"mysql">>,
    MysqlHost = ?config(mysql_host, Config0),
    MysqlPort = ?config(mysql_port, Config0),
    case emqx_common_test_helpers:is_tcp_server_available(MysqlHost, MysqlPort) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            % Ensure EE bridge module is loaded
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            % Connect to mysql directly and create the table
            connect_and_create_table(Config0),
            {Name, MysqlConfig} = mysql_config(BridgeType, Config0),
            Config =
                [
                    {mysql_config, MysqlConfig},
                    {mysql_bridge_type, BridgeType},
                    {mysql_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            {skip, no_mysql}
    end.

mysql_config(BridgeType, Config) ->
    MysqlPort = integer_to_list(?config(mysql_port, Config)),
    Server = ?config(mysql_host, Config) ++ ":" ++ MysqlPort,
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    QueryMode = ?config(query_mode, Config),
    TlsEnabled = ?config(enable_tls, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  database = ~p\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "  sql = ~p\n"
            "  resource_opts = {\n"
            "    request_timeout = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "  }\n"
            "  ssl = {\n"
            "    enable = ~w\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?MYSQL_DATABASE,
                ?MYSQL_USERNAME,
                ?MYSQL_PASSWORD,
                ?SQL_BRIDGE,
                BatchSize,
                QueryMode,
                TlsEnabled
            ]
        ),
    {Name, parse_and_check(ConfigString, BridgeType, Name)}.

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    MysqlConfig = ?config(mysql_config, Config),
    emqx_bridge:create(BridgeType, Name, MysqlConfig).

delete_bridge(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Request) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ResourceID, Request, #{timeout => 500}).

unprepare(Config, Key) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    {ok, _, #{state := #{poolname := PoolName}}} = emqx_resource:get_instance(ResourceID),
    [
        begin
            {ok, Conn} = ecpool_worker:client(Worker),
            ok = mysql:unprepare(Conn, Key)
        end
     || {_Name, Worker} <- ecpool:workers(PoolName)
    ].

% We need to create and drop the test table outside of using bridges
% since a bridge expects the table to exist when enabling it. We
% therefore call the mysql module directly, in addition to using it
% for querying the DB directly.
connect_direct_mysql(Config) ->
    Opts = [
        {host, ?config(mysql_host, Config)},
        {port, ?config(mysql_port, Config)},
        {user, ?MYSQL_USERNAME},
        {password, ?MYSQL_PASSWORD},
        {database, ?MYSQL_DATABASE}
    ],
    SslOpts =
        case ?config(enable_tls, Config) of
            true ->
                [{ssl, emqx_tls_lib:to_client_opts(#{enable => true})}];
            false ->
                []
        end,
    {ok, Pid} = mysql:start_link(Opts ++ SslOpts),
    Pid.

% These funs connect and then stop the mysql connection
connect_and_create_table(Config) ->
    DirectPid = connect_direct_mysql(Config),
    ok = mysql:query(DirectPid, ?SQL_CREATE_TABLE),
    mysql:stop(DirectPid).

connect_and_drop_table(Config) ->
    DirectPid = connect_direct_mysql(Config),
    ok = mysql:query(DirectPid, ?SQL_DROP_TABLE),
    mysql:stop(DirectPid).

connect_and_clear_table(Config) ->
    DirectPid = connect_direct_mysql(Config),
    ok = mysql:query(DirectPid, ?SQL_DELETE),
    mysql:stop(DirectPid).

connect_and_get_payload(Config) ->
    DirectPid = connect_direct_mysql(Config),
    Result = mysql:query(DirectPid, ?SQL_SELECT),
    mysql:stop(DirectPid),
    Result.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := mysql_connector_query_return},
                10_000
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[Val]]},
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    MysqlConfig0 = ?config(mysql_config, Config),
    MysqlConfig = MysqlConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(MysqlConfig)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := mysql_connector_query_return},
                10_000
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[Val]]},
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),

    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            {ok, Status} when Status =:= disconnected orelse Status =:= connecting,
            emqx_resource_manager:health_check(ResourceID)
        )
    end),
    ok.

t_create_disconnected(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ?check_trace(
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            ?assertMatch({ok, _}, create_bridge(Config))
        end),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(mysql_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_write_failure(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    QueryMode = ?config(query_mode, Config),
    {ok, _} = create_bridge(Config),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            case QueryMode of
                sync ->
                    ?assertMatch(
                        {error, {resource_error, #{reason := timeout}}},
                        send_message(Config, SentData)
                    );
                async ->
                    send_message(Config, SentData)
            end
        end),
        fun(Trace0) ->
            ct:pal("trace: ~p", [Trace0]),
            Trace = ?of_kind(buffer_worker_flush_nack, Trace0),
            ?assertMatch([#{result := {error, _}} | _], Trace),
            [#{result := {error, Error}} | _] = Trace,
            case Error of
                {resource_error, _} ->
                    ok;
                {recoverable_error, disconnected} ->
                    ok;
                _ ->
                    ct:fail("unexpected error: ~p", [Error])
            end
        end
    ),
    ok.

% This test doesn't work with batch enabled since it is not possible
% to set the timeout directly for batch queries
t_write_timeout(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    {ok, _} = create_bridge(Config),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    Timeout = 1000,
    emqx_common_test_helpers:with_failure(timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            {error, {resource_error, #{reason := timeout}}},
            query_resource(Config, {send_message, SentData, [], Timeout})
        )
    end),
    ok.

t_simple_sql_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"SELECT count(1) AS T">>},
    Result = query_resource(Config, Request),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true -> ?assertEqual({error, {unrecoverable_error, batch_select_not_implemented}}, Result);
        false -> ?assertEqual({ok, [<<"T">>], [[1]]}, Result)
    end,
    ok.

t_missing_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertMatch(
                {error,
                    {unrecoverable_error,
                        {1292, _, <<"Truncated incorrect DOUBLE value: 'undefined'">>}}},
                Result
            );
        false ->
            ?assertMatch(
                {error, {unrecoverable_error, {1048, _, <<"Column 'arrived' cannot be null">>}}},
                Result
            )
    end,
    ok.

t_bad_sql_parameter(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"">>, [bad_parameter]},
    Result = query_resource(Config, Request),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertEqual({error, {unrecoverable_error, invalid_request}}, Result);
        false ->
            ?assertEqual({error, {unrecoverable_error, {invalid_params, [bad_parameter]}}}, Result)
    end,
    ok.

t_unprepared_statement_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {prepared_query, unprepared_query, []},
    Result = query_resource(Config, Request),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true -> ?assertEqual({error, {unrecoverable_error, invalid_request}}, Result);
        false -> ?assertEqual({error, {unrecoverable_error, prepared_statement_invalid}}, Result)
    end,
    ok.

%% Test doesn't work with batch enabled since batch doesn't use
%% prepared statements as such; it has its own query generation process
t_uninitialized_prepared_statement(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    unprepare(Config, send_message),
    ?check_trace(
        begin
            ?assertEqual(ok, send_message(Config, SentData)),
            ok
        end,
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := mysql_connector_prepare_query_failed, error := not_prepared},
                    #{
                        ?snk_kind := mysql_connector_on_query_prepared_sql,
                        type_or_key := send_message
                    },
                    Trace
                )
            ),
            SendQueryTrace = ?of_kind(mysql_connector_send_query, Trace),
            ?assertMatch([#{data := [Val, _]}, #{data := [Val, _]}], SendQueryTrace),
            ReturnTrace = ?of_kind(mysql_connector_query_return, Trace),
            ?assertMatch([#{result := ok}], ReturnTrace),
            ok
        end
    ),
    ok.
