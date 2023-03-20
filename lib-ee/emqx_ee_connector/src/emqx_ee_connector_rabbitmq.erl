-module(emqx_ee_connector_rabbitmq).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
%% Needed to create connection
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(emqx_resource).
-behaviour(hocon_schema).
-behaviour(ecpool_worker).

%% hocon_schema callbacks
-export([roots/0, fields/1]).

%% HTTP API callbacks
-export([values/1]).

%% emqx_resource callbacks
-export([
    %% Required callbacks
    on_start/2,
    on_stop/2,
    callback_mode/0,
    %% Optional callbacks
    is_buffer_supported/0
]).

%% callbacks for ecpool_worker
-export([connect/1]).

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

% -define(RESOURCE_CONFIG_SPEC, #{
%         server => #{
%             order => 1,
%             type => string,
%             required => true,
%             default => <<"127.0.0.1:5672">>,
%             title => #{en => <<"RabbitMQ Server">>,
%                        zh => <<"RabbitMQ 服务器"/utf8>>},
%             description => #{en => <<"RabbitMQ Server">>,
%                              zh => <<"RabbitMQ 服务器地址"/utf8>>}
%         },
%         pool_size => #{
%             order => 3,
%             type => number,
%             required => true,
%             default => 8,
%             title => #{en => <<"Pool Size">>, zh => <<"连接池大小"/utf8>>},
%             description => #{en => <<"Size of RabbitMQ connection pool">>,
%                              zh => <<"RabbitMQ 连接池大小"/utf8>>}
%         },
%         username => #{
%             order => 4,
%             type => string,
%             default => <<"guest">>,
%             title => #{en => <<"Username">>, zh => <<"用户名"/utf8>>},
%             description => #{en => <<"RabbitMQ Username">>,
%                              zh => <<"RabbitMQ 用户名"/utf8>>}
%         },
%         password => #{
%             order => 5,
%             type => password,
%             default => <<"guest">>,
%             title => #{en => <<"Password">>, zh => <<"密码"/utf8>>},
%             description => #{en => <<"RabbitMQ Password">>,
%                              zh => <<"RabbitMQ 密码"/utf8>>}
%         },
%         timeout => #{
%             order => 6,
%             type => string,
%             default => <<"5s">>,
%             title => #{en => <<"Connection Timeout">>, zh => <<"连接超时时间"/utf8>>},
%             description => #{en => <<"Connection timeout for connecting to RabbitMQ">>,
%                              zh => <<"RabbitMQ 连接超时时间"/utf8>>}
%         },
%         virtual_host => #{
%             order => 7,
%             type => string,
%             default => <<"/">>,
%             title => #{en => <<"Virtual Host">>, zh => <<"虚拟主机"/utf8>>},
%             description => #{en => <<"Virtual host for connecting to RabbitMQ">>,
%                              zh => <<"RabbitMQ 虚拟主机"/utf8>>}
%         },
%         heartbeat => #{
%             order => 8,
%             type => string,
%             default => <<"30s">>,
%             title => #{en => <<"Heartbeart">>, zh => <<"心跳间隔"/utf8>>},
%             description => #{en => <<"Heartbeat for connecting to RabbitMQ">>,
%                              zh => <<"RabbitMQ 心跳间隔"/utf8>>}
%         },
%         auto_reconnect => #{
%             order => 9,
%             type => string,
%             default => <<"2s">>,
%             title => #{en => <<"Auto Reconnect Times">>, zh => <<"自动重连间隔"/utf8>>},
%             description => #{en => <<"Auto Reconnect Times for connecting to RabbitMQ">>,
%                              zh => <<"RabbitMQ 自动重连间隔"/utf8>>}
%         }
%     }).

% -define(ACTION_PARAM_RESOURCE, #{
%                 type => string,
%                 required => true,
%                 title => #{en => <<"Resource ID">>, zh => <<"资源 ID"/utf8>>},
%                 description => #{
%                     en => <<"Bind a resource to this action">>,
%                     zh => <<"给动作绑定一个资源"/utf8>>
%                 }
%             }).

% -resource_type(#{
%         name => ?RESOURCE_TYPE_RABBIT,
%         create => on_resource_create,
%         status => on_get_resource_status,
%         destroy => on_resource_destroy,
%         params => ?RESOURCE_CONFIG_SPEC,
%         title => #{en => <<"RabbitMQ">>, zh => <<"RabbitMQ"/utf8>>},
%         description => #{en => <<"RabbitMQ Resource">>, zh => <<"RabbitMQ 资源"/utf8>>}
%     }).

% -rule_action(#{
%         name => data_to_rabbit,
%         category => data_forward,
%         for => '$any',
%         types => [?RESOURCE_TYPE_RABBIT],
%         create => on_action_create_data_to_rabbit,
%         params => #{
%             '$resource' => ?ACTION_PARAM_RESOURCE,
%             exchange => #{
%                 order => 1,
%                 type => string,
%                 default => <<"messages">>,
%                 required => true,
%                 title => #{en => <<"RabbitMQ Exchange">>, zh => <<"RabbitMQ Exchange"/utf8>>},
%                 description => #{en => <<"RabbitMQ Exchange">>,
%                 zh => <<"RabbitMQ Exchange"/utf8>>}
%             },
%             exchange_type => #{
%                 order => 2,
%                 type => string,
%                 default => <<"topic">>,
%                 required => true,
%                 enum => [<<"direct">>, <<"fanout">>, <<"topic">>],
%                 title => #{en => <<"RabbitMQ Exchange Type">>, zh => <<"RabbitMQ Exchange Type"/utf8>>},
%                 description => #{en => <<"RabbitMQ Exchange Type">>,
%                 zh => <<"RabbitMQ Exchange 类型"/utf8>>}
%             },
%             routing_key => #{
%                 order => 3,
%                 type => string,
%                 required => true,
%                 title => #{en => <<"RabbitMQ Routing Key">>, zh => <<"Rabbit Routing Key"/utf8>>},
%                 description => #{en => <<"RabbitMQ Routing Key">>,
%                                  zh => <<"Rabbit Routing Key"/utf8>>}
%             },
%             durable => #{
%                 order => 4,
%                 type => boolean,
%                 default => false,
%                 title => #{en => <<"RabbitMQ Exchange Durable">>, zh => <<"RabbitMQ Exchange Durable"/utf8>>},
%                 description => #{en => <<"RabbitMQ Exchange Durable">>,
%                                  zh => <<"RabbitMQ Exchange Durable"/utf8>>}
%             },
%             payload_tmpl => #{
%                  order => 5,
%                  type => string,
%                  input => textarea,
%                  required => false,
%                  default => <<"">>,
%                  title => #{en => <<"Payload Template">>,
%                             zh => <<"消息内容模板"/utf8>>},
%                  description => #{en => <<"The payload template, variable interpolation is supported. If using empty template (default), then the payload will be all the available vars in JSON format">>,
%                                   zh => <<"消息内容模板，支持变量。若使用空模板（默认），消息内容为 JSON 格式的所有字段"/utf8>>}
%              }
%         },
%         title => #{
%             en => <<"Data bridge to RabbitMQ">>,
%             zh => <<"桥接数据到 RabbitMQ"/utf8>>
%         },
%         description => #{en => <<"Store Data to RabbitMQ">>,
%                          zh => <<"桥接数据到 RabbitMQ"/utf8>>}
%     }).

fields(config) ->
    [
        {server,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"localhost">>,
                    required => true,
                    desc => ?DESC("server")
                }
            )},
        {port,
            hoconsc:mk(
                typerefl:pos_integer(),
                #{
                    default => 5672,
                    required => true,
                    desc => ?DESC("server")
                }
            )},
        {username,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"guest">>,
                    required => true,
                    desc => ?DESC("username")
                }
            )},
        {password,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"guest">>,
                    required => true,
                    desc => ?DESC("password")
                }
            )},
        {pool_size,
            hoconsc:mk(
                typerefl:pos_integer(),
                #{
                    default => 8,
                    required => true,
                    desc => ?DESC("pool_size")
                }
            )},
        {timeout,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"5s">>,
                    required => true,
                    desc => ?DESC("timeout")
                }
            )},
        {virtual_host,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"/">>,
                    required => true,
                    desc => ?DESC("virtual_host")
                }
            )},
        {heartbeat,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"30s">>,
                    required => true,
                    desc => ?DESC("heartbeat")
                }
            )},
        {auto_reconnect,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"2s">>,
                    required => true,
                    desc => ?DESC("auto_reconnect")
                }
            )},
        %% Things related to sending messages to RabbitMQ
        {exchange,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"messages">>,
                    required => true,
                    desc => ?DESC("exchange")
                }
            )},
        {exchange_type,
            hoconsc:mk(
                hoconsc:enum([direct, fanout, topic]),
                #{
                    default => <<"topic">>,
                    required => true,
                    desc => ?DESC("exchange_type")
                }
            )},
        {routing_key,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"my_routing_key">>,
                    required => true,
                    desc => ?DESC("routing_key")
                }
            )},
        {durable,
            hoconsc:mk(
                hoconsc:enum([true, false]),
                #{
                    default => false,
                    required => true,
                    desc => ?DESC("durable")
                }
            )},
        {payload_template,
            hoconsc:mk(
                binary(),
                #{
                    required => true,
                    default => <<"">>,
                    desc => ?DESC("payload_template")
                }
            )}
    ].

values(post) ->
    maps:merge(values(put), #{name => <<"connector">>});
values(get) ->
    values(post);
values(put) ->
    #{
        database => <<"mqtt">>,
        enable => true,
        pool_size => 8,
        type => clickhouse,
        url => <<"http://127.0.0.1:8123">>
    };
values(_) ->
    #{}.

%% ===================================================================
%% Callbacks defined in emqx_resource
%% ===================================================================

callback_mode() -> always_sync.

is_buffer_supported() ->
    %% We want to make use of EMQX's buffer mechanism
    false.

-spec on_start(resource_id(), term()) -> {ok, term()} | {error, _}.

on_start(
    InstanceID,
    #{
        pool_size := PoolSize
    } = Config
) ->
    erlang:display({on_start, InstanceID, Config}),
    ?SLOG(info, #{
        msg => "starting_rabbitmq_connector",
        connector => InstanceID,
        config => emqx_misc:redact(Config)
    }),
    PoolName = emqx_plugin_libs_pool:pool_name(InstanceID),
    Options = [
        {config, Config},
        %% The pool_size is read by ecpool and decides the number of workers in
        %% the pool
        {pool_size, PoolSize},
        {pool, PoolName}
    ],
    State = #{poolname => PoolName},
    case emqx_plugin_libs_pool:start_pool(PoolName, ?MODULE, Options) of
        ok ->
            {ok, State};
        {error, Reason} ->
            LogMessage =
                #{
                    msg => "rabbitmq_connector_start_failed",
                    error_reason => Reason,
                    config => emqx_misc:redact(Config)
                },
            ?SLOG(info, LogMessage),
            {error, Reason}
    end.

on_stop(
    _InstanceID,
    #{
        server := _Server,
        port := _Port,
        pool_size := _PoolSize
    } = _Config
) ->
    erlang:display({on_stop}),
    ok.

connect(Options) ->
    Config = proplists:get_value(config, Options),
    #{
        server := Host,
        port := Port,
        username := Username,
        password := Password,
        timeout := Timeout,
        virtual_host := VirtualHost,
        heartbeat := Heartbeat
    } = Config,
    %XX {auto_reconnect => 2000,
    %XX  durable => false,
    %XX  exchange => <<"test_exchange">>,
    %XX  exchange_type => topic,
    %XX  heartbeat => 30000,
    %XX  password => <<"guest">>,
    %XX  payload_template => <<>>,
    %  pool_size => 8,
    %XX  port => 5672,
    %  routing_key => <<"test_routing_key">>,
    %  server => <<"localhost">>,
    %  timeout => 5000,
    %  username => <<"guest">>,
    %  virtual_host => <<"/">>}
    RabbitMQConnectionOptions =
        #amqp_params_network{
            host = erlang:binary_to_list(Host),
            port = Port,
            username = Username,
            password = Password,
            connection_timeout = Timeout,
            virtual_host = VirtualHost,
            heartbeat = Heartbeat
        },
    {ok, RabbitMQConnection} = amqp_connection:start(RabbitMQConnectionOptions),
    {ok, RabbitMQChannel} = amqp_connection:open_channel(RabbitMQConnection),
    {ok,
        #{
            connection => RabbitMQConnection,
            channel => RabbitMQChannel
        },
        #{supervisees => [RabbitMQConnection, RabbitMQChannel]}}.
