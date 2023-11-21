%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql_schema).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_postgresql/include/emqx_postgresql.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("epgsql/include/epgsql.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([roots/0, fields/1]).

%% Examples
-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

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
    emqx_bridge_v2_schema:make_producer_action_schema(hoconsc:ref(?MODULE, action_parameters));
%% TODO: All of these needs to be fixed
fields("put_bridge_v2") ->
    fields(pgsql_action);
fields("get_bridge_v2") ->
    fields(pgsql_action);
fields("post_bridge_v2") ->
    fields(pgsql_action);
fields("put_connector") ->
    fields("config_connector");
fields("get_connector") ->
    fields("config_connector");
fields("post_connector") ->
    fields("config_connector").

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

%% Examples

connector_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Producer Connector">>,
                value => values({Method, connector})
            }
        }
    ].

bridge_v2_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Producer Action">>,
                value => values({Method, bridge_v2_producer})
            }
        }
    ].

conn_bridge_examples(Method) ->
    [
        #{
            <<"pgsql">> => #{
                summary => <<"PostgreSQL Producer Bridge">>,
                value => values({Method, producer})
            }
        }
    ].

%% TODO: All of these needs to be adjusted from Kafka to PostgreSQL
values({get, PostgreSQLType}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ]
        },
        values({post, PostgreSQLType})
    );
values({post, connector}) ->
    maps:merge(
        #{
            name => <<"my_pgsql_connector">>,
            type => <<"pgsql">>
        },
        values(common_config)
    );
values({post, PostgreSQLType}) ->
    maps:merge(
        #{
            name => <<"my_pgsql_action">>,
            type => <<"pgsql">>
        },
        values({put, PostgreSQLType})
    );
values({put, bridge_v2_producer}) ->
    values(bridge_v2_producer);
values({put, connector}) ->
    values(common_config);
values({put, PostgreSQLType}) ->
    maps:merge(values(common_config), values(PostgreSQLType));
values(bridge_v2_producer) ->
    maps:merge(
        #{
            enable => true,
            connector => <<"my_pgsql_connector">>,
            resource_opts => #{
                health_check_interval => "32s"
            }
        },
        values(producer)
    );
values(common_config) ->
    #{
        authentication => #{
            mechanism => <<"plain">>,
            username => <<"username">>,
            password => <<"******">>
        },
        bootstrap_hosts => <<"localhost:9092">>,
        connect_timeout => <<"5s">>,
        enable => true,
        metadata_request_timeout => <<"4s">>,
        min_metadata_refresh_interval => <<"3s">>,
        socket_opts => #{
            sndbuf => <<"1024KB">>,
            recbuf => <<"1024KB">>,
            nodelay => true,
            tcp_keepalive => <<"none">>
        }
    };
values(producer) ->
    #{
        kafka => #{
            topic => <<"kafka-topic">>,
            message => #{
                key => <<"${.clientid}">>,
                value => <<"${.}">>,
                timestamp => <<"${.timestamp}">>
            },
            max_batch_bytes => <<"896KB">>,
            compression => <<"no_compression">>,
            partition_strategy => <<"random">>,
            required_acks => <<"all_isr">>,
            partition_count_refresh_interval => <<"60s">>,
            kafka_headers => <<"${pub_props}">>,
            kafka_ext_headers => [
                #{
                    kafka_ext_header_key => <<"clientid">>,
                    kafka_ext_header_value => <<"${clientid}">>
                },
                #{
                    kafka_ext_header_key => <<"topic">>,
                    kafka_ext_header_value => <<"${topic}">>
                }
            ],
            kafka_header_value_encode_mode => none,
            max_inflight => 10,
            buffer => #{
                mode => <<"hybrid">>,
                per_partition_limit => <<"2GB">>,
                segment_bytes => <<"100MB">>,
                memory_overload_protection => true
            }
        },
        local_topic => <<"mqtt/local/topic">>
    }.
