%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_kinesis).
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% hocon_schema API
-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-export([
    bridge_v2_examples/1,
    conn_bridge_examples/1,
    connector_examples/1
]).

%%-------------------------------------------------------------------------------------------------
%% `hocon_schema' API
%%-------------------------------------------------------------------------------------------------

namespace() ->
    "bridge_kinesis".

roots() ->
    [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    emqx_connector_schema:api_fields(
        Field,
        kinesis,
        connector_config_fields()
    );
fields(action) ->
    {kinesis,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, kinesis_action)),
            #{
                desc => <<"Kinesis Action Config">>,
                required => false
            }
        )};
fields(action_parameters) ->
    fields(producer);
fields(kinesis_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => ?DESC("action_parameters")
            }
        )
    );
fields("config_producer") ->
    emqx_bridge_schema:common_bridge_fields() ++
        fields("resource_opts") ++
        fields(connector_config) ++
        fields(producer);
fields("resource_opts") ->
    [
        {resource_opts,
            mk(
                ref(?MODULE, "creation_opts"),
                #{
                    required => false,
                    default => #{},
                    desc => ?DESC(emqx_resource_schema, "creation_opts")
                }
            )}
    ];
fields("creation_opts") ->
    emqx_resource_schema:create_opts([
        {batch_size, #{
            validator => emqx_resource_validator:max(int, 500)
        }}
    ]);
fields(connector_config) ->
    [
        {aws_access_key_id,
            mk(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("aws_access_key_id")
                }
            )},
        {aws_secret_access_key,
            emqx_schema_secret:mk(
                #{
                    required => true,
                    desc => ?DESC("aws_secret_access_key")
                }
            )},
        {endpoint,
            mk(
                binary(),
                #{
                    required => true,
                    example => <<"https://kinesis.us-east-1.amazonaws.com">>,
                    desc => ?DESC("endpoint")
                }
            )},
        {max_retries,
            mk(
                non_neg_integer(),
                #{
                    required => false,
                    default => 2,
                    desc => ?DESC("max_retries")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )}
    ];
fields(producer) ->
    [
        {payload_template,
            sc(
                binary(),
                #{
                    default => <<"${.}">>,
                    desc => ?DESC("payload_template")
                }
            )},
        {local_topic,
            sc(
                binary(),
                #{
                    desc => ?DESC("local_topic")
                }
            )},
        {stream_name,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("stream_name")
                }
            )},
        {partition_key,
            sc(
                binary(),
                #{
                    required => true,
                    desc => ?DESC("partition_key")
                }
            )}
    ];
fields("get_producer") ->
    emqx_bridge_schema:status_fields() ++ fields("post_producer");
fields("post_producer") ->
    [type_field_producer(), name_field() | fields("config_producer")];
fields("put_producer") ->
    fields("config_producer");
fields("config_connector") ->
    connector_config_fields();
fields("put_bridge_v2") ->
    fields(kinesis_action);
fields("get_bridge_v2") ->
    fields(kinesis_action);
fields("post_bridge_v2") ->
    fields("post", kinesis, kinesis_action).

fields("post", Type, StructName) ->
    [type_field(Type), name_field() | fields(StructName)].

type_field(Type) ->
    {type, hoconsc:mk(hoconsc:enum([Type]), #{required => true, desc => ?DESC("desc_type")})}.

desc("config_producer") ->
    ?DESC("desc_config");
desc("creation_opts") ->
    ?DESC(emqx_resource_schema, "creation_opts");
desc(_) ->
    undefined.

conn_bridge_examples(Method) ->
    [
        #{
            <<"kinesis_producer">> => #{
                summary => <<"Amazon Kinesis Producer Bridge">>,
                value => values(producer, Method)
            }
        }
    ].

%% TODO Fix these examples

connector_examples(Method) ->
    [
        #{
            <<"kinesis">> => #{
                summary => <<"Kinesis Connector">>,
                value => values({Method, connector})
            }
        }
    ].

bridge_v2_examples(Method) ->
    [
        #{
            <<"kinesis">> => #{
                summary => <<"Kinesis Action">>,
                value => values({Method, bridge_v2_producer})
            }
        }
    ].

values({get, connector}) ->
    maps:merge(
        #{
            status => <<"connected">>,
            node_status => [
                #{
                    node => <<"emqx@localhost">>,
                    status => <<"connected">>
                }
            ],
            actions => [<<"my_action">>]
        },
        values({post, connector})
    );
values({get, KafkaType}) ->
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
        values({post, KafkaType})
    );
values({post, connector}) ->
    maps:merge(
        #{
            name => <<"my_kinesis_connector">>,
            type => <<"kinesis">>
        },
        values(common_config)
    );
values({post, KafkaType}) ->
    maps:merge(
        #{
            name => <<"my_kafka_producer_action">>,
            type => <<"kafka_producer">>
        },
        values({put, KafkaType})
    );
values({put, bridge_v2_producer}) ->
    values(bridge_v2_producer);
values({put, connector}) ->
    values(common_config);
values({put, KafkaType}) ->
    maps:merge(values(common_config), values(KafkaType));
values(bridge_v2_producer) ->
    #{
        enable => true,
        connector => <<"my_kinesis_connector">>,
        parameters => values(producer_values),
        resource_opts => #{
            <<"batch_size">> => 100,
            <<"inflight_window">> => 100,
            <<"max_buffer_bytes">> => <<"256MB">>,
            <<"request_ttl">> => <<"45s">>
        }
    };
values(common_config) ->
    #{
        <<"enable">> => true,
        <<"aws_access_key_id">> => <<"your_access_key">>,
        <<"aws_secret_access_key">> => <<"aws_secret_key">>,
        <<"endpoint">> => <<"http://localhost:4566">>,
        <<"max_retries">> => 2,
        <<"pool_size">> => 8
    };
values(producer_values) ->
    #{
        <<"partition_key">> => <<"any_key">>,
        <<"payload_template">> => <<"${.}">>,
        <<"stream_name">> => <<"my_stream">>
    }.

values(producer, _Method) ->
    #{
        aws_access_key_id => <<"aws_access_key_id">>,
        aws_secret_access_key => <<"******">>,
        endpoint => <<"https://kinesis.us-east-1.amazonaws.com">>,
        max_retries => 3,
        stream_name => <<"stream_name">>,
        partition_key => <<"key">>,
        resource_opts => #{
            worker_pool_size => 1,
            health_check_interval => 15000,
            query_mode => async,
            inflight_window => 100,
            max_buffer_bytes => 100 * 1024 * 1024
        }
    }.

% #{<<"actions">> =>
%       #{<<"kinesis">> =>
%             #{<<"kinesis_test">> =>
%                   #{<<"connector">> => <<"kinesis_test">>,<<"enable">> => true,
%                     <<"parameters">> =>
%                         ,
%                     <<"resource_opts">> =>
%                         #{<<"batch_size">> => 1,
%                           <<"health_check_interval">> => <<"15s">>,
%                           <<"inflight_window">> => 100,
%                           <<"max_buffer_bytes">> => <<"256MB">>,
%                           <<"query_mode">> => <<"async">>,
%                           <<"request_ttl">> => <<"45s">>,
%                           <<"worker_pool_size">> => 16}}}},
%   <<"bridges">> => #{<<"kinesis_producer">> => #{}},
%   <<"cluster">> =>
%       #{<<"discovery_strategy">> => <<"manual">>,
%         <<"name">> => <<"emqxcl">>},
%   <<"connectors">> =>
%       #{<<"kinesis">> =>
%             #{<<"kinesis_test">> =>
%                   #{<<"aws_access_key_id">> => <<"any_value">>,
%                     <<"aws_secret_access_key">> => <<"any_value">>,
%                     <<"enable">> => true,
%                     <<"endpoint">> => <<"http://localhost:4566">>,
%                     <<"max_retries">> => 2,<<"pool_size">> => 8}}},

%%-------------------------------------------------------------------------------------------------
%% Helper fns
%%-------------------------------------------------------------------------------------------------

connector_config_fields() ->
    fields(connector_config).

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

mk(Type, Meta) -> hoconsc:mk(Type, Meta).

enum(OfSymbols) -> hoconsc:enum(OfSymbols).

ref(Module, Name) -> hoconsc:ref(Module, Name).

type_field_producer() ->
    {type, mk(enum([kinesis_producer]), #{required => true, desc => ?DESC("desc_type")})}.

name_field() ->
    {name, mk(binary(), #{required => true, desc => ?DESC("desc_name")})}.
