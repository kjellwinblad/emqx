%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rocketmq).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx_bridge/include/emqx_bridge.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-import(hoconsc, [mk/2, enum/1]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    bridge_v2_examples/1,
    connector_examples/1,
    conn_bridge_examples/1,
    values/1
]).

-define(CONNECTOR_TYPE, rocketmq).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).
-define(DEFAULT_TEMPLATE, <<>>).
-define(DEFFAULT_REQ_TIMEOUT, <<"15s">>).

%% -------------------------------------------------------------------------------------------------
%% api

conn_bridge_examples(Method) ->
    [
        #{
            <<"rocketmq">> => #{
                summary => <<"RocketMQ Bridge">>,
                value => values(Method)
            }
        }
    ].

values(get) ->
    values(post);
values(post) ->
    #{
        enable => true,
        type => rocketmq,
        name => <<"foo">>,
        server => <<"127.0.0.1:9876">>,
        topic => <<"TopicTest">>,
        template => ?DEFAULT_TEMPLATE,
        local_topic => <<"local/topic/#">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => ?HEALTHCHECK_INTERVAL_RAW,
            batch_size => ?DEFAULT_BATCH_SIZE,
            batch_time => ?DEFAULT_BATCH_TIME,
            query_mode => sync,
            max_buffer_bytes => ?DEFAULT_BUFFER_BYTES
        }
    };
values(put) ->
    values(post).

%% TODO fix these examples

connector_examples(Method) ->
    [
        #{
            <<"oracle">> =>
                #{
                    summary => <<"Oracle Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_values()
                    )
                }
        }
    ].

connector_values() ->
    #{
        <<"username">> => <<"system">>,
        <<"password">> => <<"oracle">>,
        <<"server">> => <<"127.0.0.1:1521">>,
        <<"service_name">> => <<"XE">>,
        <<"sid">> => <<"XE">>,
        <<"pool_size">> => 8,
        <<"resource_opts">> =>
            #{
                <<"health_check_interval">> => <<"15s">>,
                <<"start_timeout">> => <<"5s">>
            }
    }.

bridge_v2_examples(Method) ->
    [
        #{
            <<"oracle">> =>
                #{
                    summary => <<"Oracle Action">>,
                    value => emqx_bridge_v2_schema:action_values(
                        Method, ?ACTION_TYPE, ?CONNECTOR_TYPE, action_values()
                    )
                }
        }
    ].

action_values() ->
    #{
        parameters => #{
            <<"sql">> => <<"F">>
        }
    }.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions
namespace() -> "bridge_rocketmq".

roots() -> [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        ?CONNECTOR_TYPE,
        fields("config_connector")
    );
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(rocketmq_action));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, rocketmq_action)),
            #{
                desc => <<"RocketMQ Action Config">>,
                required => false
            }
        )};
fields(rocketmq_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields(action_parameters) ->
    Parameters =
        [
            {template,
                mk(
                    binary(),
                    #{desc => ?DESC("template"), default => ?DEFAULT_TEMPLATE}
                )}
        ] ++ emqx_bridge_rocketmq_connector:fields(config),
    lists:foldl(
        fun(Key, Acc) ->
            proplists:delete(Key, Acc)
        end,
        Parameters,
        [
            servers,
            pool_size,
            auto_reconnect,
            access_key,
            secret_key,
            security_token
        ]
    );
fields("config_connector") ->
    Config =
        emqx_connector_schema:common_fields() ++
            emqx_bridge_rocketmq_connector:fields(config) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    lists:foldl(
        fun(Key, Acc) ->
            proplists:delete(Key, Acc)
        end,
        Config,
        [
            topic,
            sync_timeout,
            refresh_interval,
            send_buffer,
            auto_reconnect
        ]
    );
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    [
        {enable, mk(boolean(), #{desc => ?DESC("config_enable"), default => true})},
        {template,
            mk(
                binary(),
                #{desc => ?DESC("template"), default => ?DEFAULT_TEMPLATE}
            )},
        {local_topic,
            mk(
                binary(),
                #{desc => ?DESC("local_topic"), required => false}
            )}
    ] ++ emqx_resource_schema:fields("resource_opts") ++
        (emqx_bridge_rocketmq_connector:fields(config) --
            emqx_connector_schema_lib:prepare_statement_fields());
fields("post") ->
    [type_field(), name_field() | fields("config")];
fields("put") ->
    fields("config");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc("config") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for RocketMQ using `", string:to_upper(Method), "` method."];
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc("config_connector") ->
    ?DESC("config_connector");
desc(oracle_action) ->
    ?DESC("oracle_action");
desc(action_parameters) ->
    ?DESC("action_parameters");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc(_) ->
    undefined.

%% -------------------------------------------------------------------------------------------------

type_field() ->
    {type, mk(enum([rocketmq]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
