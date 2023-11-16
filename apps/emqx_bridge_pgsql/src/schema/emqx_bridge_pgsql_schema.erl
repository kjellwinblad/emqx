%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql_schema).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([roots/0, fields/1]).

-define(PGSQL_HOST_OPTIONS, #{
    default_port => ?PGSQL_DEFAULT_PORT
}).

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields("config_connector") ->
    [{server, server()}] ++
        adjust_fields(emqx_connector_schema_lib:relational_db_fields()) ++
        emqx_connector_schema_lib:ssl_fields();
fields(config) ->
    fields("config_connector") ++
        fields(action);
fields(action) ->
    {pgsql,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_pgsql_schema, pgsql_action)),
            #{
                desc => <<"PostgreSQL Action Config">>,
                required => false
            }
        )};
fields(action_parameters) ->
    [
        {sql,
            hoconsc:mk(
                binary(),
                #{desc => ?DESC("sql_template"), default => default_sql(), format => <<"sql">>}
            )}
    ] ++
        emqx_connector_schema_lib:prepare_statement_fields();
fields(pgsql_action) ->
    emqx_bridge_v2_schema:make_action_schema(hoconsc:ref(?MODULE, action_parameters)).

server() ->
    Meta = #{desc => ?DESC("server")},
    emqx_schema:servers_sc(Meta, ?PGSQL_HOST_OPTIONS).

adjust_fields(Fields) ->
    lists:map(
        fun
            ({username, Sc}) ->
                %% to please dialyzer...
                Override = #{type => hocon_schema:field_schema(Sc, type), required => true},
                {username, hocon_schema:override(Sc, Override)};
            (Field) ->
                Field
        end,
        Fields
    ).

default_sql() ->
    <<
        "insert into t_mqtt_msg(msgid, topic, qos, payload, arrived) "
        "values (${id}, ${topic}, ${qos}, ${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
    >>.
