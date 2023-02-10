%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_clickhouse_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-define(CLICKHOUSE_HOST, "localhost").
-define(CLICKHOUSE_RESOURCE_MOD, emqx_ee_connector_clickhouse).
-include_lib("emqx_connector/include/emqx_connector.hrl").

sql_for_bridge() ->
    "INSERT INTO mqtt_test(payload, arrived) "
    "VALUES ('${payload}', FROM_UNIXTIME(${timestamp}))".

sql_create_table() ->
    "CREATE TABLE IF NOT EXISTS mqtt.mqtt_test (payload String, arrived datetime NOT NULL) ENGINE = Memory".

clickhouse_url() ->
    erlang:iolist_to_binary([
        <<"http://">>,
        ?CLICKHOUSE_HOST,
        ":",
        erlang:integer_to_list(?CLICKHOUSE_DEFAULT_PORT)
    ]).

%% See comment in
%% lib-ee/emqx_ee_connector/test/ee_connector_clickhouse_SUITE.erl for how to
%% run this without bringing up the whole CI infrastucture

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

% start_apps() ->
%     ensure_loaded(),
%     %% some configs in emqx_conf app are mandatory,
%     %% we want to make sure they are loaded before
%     %% ekka start in emqx_common_test_helpers:start_apps/1
%     emqx_common_test_helpers:render_and_load_app_config(emqx_conf),
%     ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]).

% ensure_loaded() ->
%     _ = application:load(emqx_ee_bridge),
%     _ = emqx_ee_bridge:module_info(),
%     ok.
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
            {ok, Conn} =
                clickhouse:start_link([
                    {url, clickhouse_url()},
                    {user, <<"default">>},
                    {key, "public"},
                    {pool, tmp_pool}
                ]),
            {ok, _, _} = clickhouse:query(Conn, <<"CREATE DATABASE IF NOT EXISTS mqtt">>, #{}),
            {ok, _, _} = clickhouse:query(Conn, sql_create_table(), #{}),
            Config;
        false ->
            {skip, no_clickhouse}
    end.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    _ = application:stop(emqx_ee_connector),
    _ = application:stop(emqx_bridge).

all() ->
    [
        create_delete_bridge,
        send
    ].

clickhouse_config(Config) ->
    BatchSize = proplists:get_value(batch_size, Config, 1),
    QueryMode = proplists:get_value(query_mode, Config, sync),
    UseTLS = proplists:get_value(use_tls, Config, false),
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
            "    query_mode = ~s\n"
            % "    batch_size = ~b\n"
            "  }\n"
            "}\n",
            [
                Name,
                URL,
                sql_for_bridge(),
                QueryMode
                % BatchSize
            ]
        ),
    parse_and_check(ConfigString, <<"clickhouse">>, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

create_delete_bridge(_Config) ->
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    show(bofore_crash),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        clickhouse_config([])
    ),
    {ok, _} = emqx_bridge:remove(Type, Name),
    ok.

show(X) ->
    erlang:display(X),
    X.

show(Label, What) ->
    erlang:display({Label, What}),
    What.

send(_Config) ->
    % erlang:display({logger:get_handler_config()}),
    % logger:set_application_level(emqx_ee_connector, debug),
    % logger:set_application_level(emqx_ee_bridge, debug),
    % logger:add_handler(my_handler_id, logger_std_h, #{}),
    % application:set_env(kernel, logger_level, debug),
    % application:set_env(kernel, logger,
    %                     [
    %                      %% Console logger
    %                      {handler, default, logger_std_h,
    %                       #{formatter => {flatlog, #{
    %                                                  map_depth => 3,
    %                                                  term_depth => 50
    %                                                 }}}
    %                      }]),
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    erlang:display({
        before_create_wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww
    }),
    {ok, X} = emqx_bridge:create(
        Type,
        Name,
        clickhouse_config([])
    ),
    BridgeID = show(emqx_bridge_resource:bridge_id(Type, Name)),
    % emqx_bridge
    Payload = #{payload => <<"clickhouse_data">>, timestamp => 10000},
    show(listing, emqx_bridge:list()),
    ok = show(message_eeeeeeeeeeeeeeeeeeeeeeeeee, emqx_bridge:send_message(BridgeID, Payload)),
    {ok, _} = emqx_bridge:remove(Type, Name),
    ok.
