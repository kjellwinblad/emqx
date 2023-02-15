%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_clickhouse_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-define(CLICKHOUSE_HOST, "localhost").
-define(CLICKHOUSE_RESOURCE_MOD, emqx_ee_connector_clickhouse).
-include_lib("emqx_connector/include/emqx_connector.hrl").

%% See comment in
%% lib-ee/emqx_ee_connector/test/ee_connector_clickhouse_SUITE.erl for how to
%% run this without bringing up the whole CI infrastucture

%%------------------------------------------------------------------------------
%% Common Test Setup, Teardown and Testcase List
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    case
        emqx_common_test_helpers:is_tcp_server_available(?CLICKHOUSE_HOST, ?CLICKHOUSE_DEFAULT_PORT)
    of
        true ->
            emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            {ok, _} = application:ensure_all_started(emqx_ee_connector),
            {ok, _} = application:ensure_all_started(emqx_ee_bridge),
            snabbkaffe:fix_ct_logging(),
            %% Create the db table
            InitPerSuiteProcess = self(),
            %% Start clickhouse connector in sub process so that it does not go
            %% down with the process that is calling  init_per_suite
            erlang:spawn(
                fun() ->
                    {ok, Conn} =
                        clickhouse:start_link([
                            {url, clickhouse_url()},
                            {user, <<"default">>},
                            {key, "public"},
                            {pool, tmp_pool}
                        ]),
                    InitPerSuiteProcess ! {clickhouse_connection, Conn},
                    Ref = erlang:monitor(process, Conn),
                    receive
                        {'DOWN', Ref, process, _, _} ->
                            erlang:display(helper_down),
                            ok
                    end
                end
            ),
            Conn =
                receive
                    {clickhouse_connection, C} -> C
                end,
            % erlang:monitor,sb
            {ok, _, _} = clickhouse:query(Conn, sql_create_database(), #{}),
            {ok, _, _} = clickhouse:query(Conn, sql_create_table(), []),
            show(saving_clickhouse_connection, Conn),
            show(saving_clickhouse_connection_test, clickhouse:query(Conn, sql_find_key(42), [])),
            show(saving_clickhouse_connection_state, sys:get_state(Conn)),
            [{clickhouse_connection, Conn} | Config];
        false ->
            {skip, no_clickhouse}
    end.

end_per_suite(Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    show(stopping_clickhouse_connection, ClickhouseConnection),
    clickhouse:stop(ClickhouseConnection),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_ee_connector),
    _ = application:stop(emqx_bridge).

all() ->
    [
        t_make_delete_bridge,
        t_send_message_query,
        t_send_simple_batch,
        t_heavy_batching
    ].

%%------------------------------------------------------------------------------
%% Helper functions for test cases
%%------------------------------------------------------------------------------

sql_insert_tmeplate_for_bridge() ->
    "INSERT INTO mqtt_test(key, data, arrived) "
    "VALUES (${key}, '${data}', ${timestamp})".

sql_create_table() ->
    "CREATE TABLE IF NOT EXISTS mqtt.mqtt_test (key BIGINT, data String, arrived BIGINT) ENGINE = Memory".

sql_find_key(Key) ->
    io_lib:format("SELECT key FROM mqtt.mqtt_test WHERE key = ~p", [Key]).

sql_find_all_keys() ->
    "SELECT key FROM mqtt.mqtt_test".

sql_drop_table() ->
    "DROP TABLE IF EXISTS mqtt.mqtt_test".

sql_create_database() ->
    "CREATE DATABASE IF NOT EXISTS mqtt".

clickhouse_url() ->
    erlang:iolist_to_binary([
        <<"http://">>,
        ?CLICKHOUSE_HOST,
        ":",
        erlang:integer_to_list(?CLICKHOUSE_DEFAULT_PORT)
    ]).

clickhouse_config(Config) ->
    BatchSize = maps:get(batch_size, Config, 1),
    BatchTime = maps:get(batch_time_ms, Config, 0),
    Name = atom_to_binary(?MODULE),
    URL = clickhouse_url(),
    ConfigString =
        io_lib:format(
            "bridges.clickhouse.~s {\n"
            "  enable = true\n"
            "  url = \"~s\"\n"
            "  database = \"mqtt\"\n"
            "  sql = \"~s\"\n"
            "  resource_opts = {\n"
            "    batch_size = ~b\n"
            "    batch_time = ~bms\n"
            "  }\n"
            "}\n",
            [
                Name,
                URL,
                sql_insert_tmeplate_for_bridge(),
                BatchSize,
                BatchTime
            ]
        ),
    parse_and_check(ConfigString, <<"clickhouse">>, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

show(X) ->
    erlang:display(X),
    X.

show(Label, What) ->
    erlang:display({Label, What}),
    What.

make_bridge(_Config) ->
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    BridgeConfig = clickhouse_config(#{}),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        BridgeConfig
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

delete_bridge() ->
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    {ok, _} = emqx_bridge:remove(Type, Name),
    ok.

reset_table(Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_drop_table(), []),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_create_table(), []),
    ok.

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_make_delete_bridge(_Config) ->
    make_bridge(#{}),
    %% Check that the new brige is in the list of bridges
    Bridges = emqx_bridge:list(),
    Name = atom_to_binary(?MODULE),
    IsRightName =
        fun
            (#{name := BName}) when BName =:= Name ->
                true;
            (_) ->
                false
        end,
    true = lists:any(IsRightName, Bridges),
    delete_bridge(),
    BridgesAfterDelete = emqx_bridge:list(),
    false = lists:any(IsRightName, BridgesAfterDelete),
    ok.

t_send_message_query(Config) ->
    reset_table(Config),
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    BridgeID = make_bridge(#{}),
    Payload = #{key => 42, data => <<"clickhouse_data">>, timestamp => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    {ok, 200, ResultString} = clickhouse:query(ClickhouseConnection, sql_find_key(42), []),
    <<"42">> = iolist_to_binary(string:trim(ResultString)),
    delete_bridge(),
    reset_table(Config),
    ok.

t_send_simple_batch(Config) ->
    reset_table(Config),
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    BridgeID = make_bridge(#{batch_size => 100}),
    Payload = #{key => 42, data => <<"clickhouse_data">>, timestamp => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    {ok, 200, ResultString} = clickhouse:query(ClickhouseConnection, sql_find_key(42), []),
    <<"42">> = iolist_to_binary(string:trim(ResultString)),
    delete_bridge(),
    reset_table(Config),
    ok.

t_heavy_batching(Config) ->
    reset_table(Config),
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    BatchTime = 500,
    NumberOfMessages = 10000,
    BridgeID = make_bridge(#{batch_size => 100, batch_time_ms => BatchTime}),
    SendMessageKey = fun(Key) ->
        Payload = #{
            key => Key,
            data => <<"clickhouse_data">>,
            timestamp => 10000
        },
        emqx_bridge:send_message(BridgeID, Payload)
    end,
    [SendMessageKey(Key) || Key <- lists:seq(1, NumberOfMessages)],
    timer:sleep(BatchTime * 5),
    {ok, 200, ResultString1} = clickhouse:query(ClickhouseConnection, sql_find_all_keys(), []),
    ResultString2 = iolist_to_binary(string:trim(ResultString1)),
    KeyStrings = string:split(ResultString2, "\n"),
    Keys = [erlang:list_to_integer(KeyString) || KeyString <- KeyStrings],
    KeySet = maps:from_keys(Keys, true),
    NumberOfMessages = maps:size(KeySet),
    CheckKey = fun(Key) -> maps:get(Key, KeySet, false) end,
    true = lists:all(CheckKey, lists:seq(1, NumberOfMessages)),
    delete_bridge(),
    reset_table(Config),
    ok.
