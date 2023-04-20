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
    on_get_status/2,
    on_query/3,
    is_buffer_supported/0,
    on_batch_query/3
]).

%% callbacks for ecpool_worker
-export([connect/1]).

roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

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
        {wait_for_publish_confirmations,
            hoconsc:mk(
                boolean(),
                #{
                    required => true,
                    default => true,
                    desc => ?DESC("wait_for_publish_confirmations")
                }
            )},
        {publish_confirmation_timeout,
            hoconsc:mk(
                emqx_schema:duration_ms(),
                #{
                    default => <<"30s">>,
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
        %% Needed?
        % {exchange_type,
        %     hoconsc:mk(
        %         hoconsc:enum([direct, fanout, topic]),
        %         #{
        %             default => <<"topic">>,
        %             required => true,
        %             desc => ?DESC("exchange_type")
        %         }
        %     )},
        {routing_key,
            hoconsc:mk(
                typerefl:binary(),
                #{
                    default => <<"my_routing_key">>,
                    required => true,
                    desc => ?DESC("routing_key")
                }
            )},
        {delivery_mode,
            hoconsc:mk(
                hoconsc:enum([1, 2]),
                #{
                    default => 1,
                    required => true,
                    desc => ?DESC("delivery_mode")
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
        pool_size := PoolSize,
        payload_template := PayloadTemplate
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
    ProcessedTemplate = emqx_plugin_libs_rule:preproc_tmpl(PayloadTemplate),
    State = #{
        poolname => PoolName,
        processed_payload_template => ProcessedTemplate,
        config => Config
    },
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
    ResourceID,
    #{poolname := PoolName}
) ->
    ?SLOG(info, #{
        msg => "stopping RabbitMQ connector",
        connector => ResourceID
    }),
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    Clients = [
        begin
            {ok, Client} = ecpool_worker:client(Worker),
            Client
        end
     || Worker <- Workers
    ],
    erlang:display({on_stopxx, ResourceID, Clients}),
    %% We need to stop the pool before stopping the workers as the pool monitors the workers
    StopResult = emqx_plugin_libs_pool:stop_pool(PoolName),
    erlang:display({on_stop, ResourceID, StopResult}),
    % erlang:display({on_get_status, Clients}),
    [
        stop_worker(Client)
     || Client <- Clients
    ],
    StopResult.

stop_worker(#{
    channel := Channel,
    connection := Connection
}) ->
    amqp_channel:close(Channel),
    amqp_connection:close(Connection).

connect(Options) ->
    Config = proplists:get_value(config, Options),
    #{
        server := Host,
        port := Port,
        username := Username,
        password := Password,
        timeout := Timeout,
        virtual_host := VirtualHost,
        heartbeat := Heartbeat,
        wait_for_publish_confirmations := WaitForPublishConfirmations
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
    %% We want to get confirmations from the server that the messages were received
    case WaitForPublishConfirmations of
        true -> #'confirm.select_ok'{} = amqp_channel:call(RabbitMQChannel, #'confirm.select'{});
        false -> ok
    end,
    {ok,
        #{
            connection => RabbitMQConnection,
            channel => RabbitMQChannel
        },
        #{supervisees => [RabbitMQConnection, RabbitMQChannel]}}.

on_get_status(
    _InstId,
    #{
        poolname := PoolName
    } = State
) ->
    erlang:display({on_get_status, PoolName}),
    Workers = [Worker || {_WorkerName, Worker} <- ecpool:workers(PoolName)],
    Clients = [
        begin
            {ok, Client} = ecpool_worker:client(Worker),
            Client
        end
     || Worker <- Workers
    ],
    erlang:display({on_get_status, Clients}),
    CheckResults = [
        check_worker(Client)
     || Client <- Clients
    ],
    erlang:display({on_get_status2, CheckResults}),
    Connected = length(CheckResults) > 0 andalso lists:all(fun(R) -> R end, CheckResults),
    case Connected of
        true ->
            {connected, State};
        false ->
            {disconnect, State, <<"not_connected">>}
    end.

check_worker(#{
    channel := Channel,
    connection := Connection
}) ->
    erlang:is_process_alive(Channel) andalso erlang:is_process_alive(Connection).

on_query(
    ResourceID,
    {RequestType, Data},
    #{
        poolname := PoolName,
        processed_payload_template := PayloadTemplate,
        config := Config
    } = State
) ->
    ?SLOG(debug, #{
        msg => "RabbitMQ connector received query",
        connector => ResourceID,
        type => RequestType,
        sql => Data,
        state => State
    }),
    #{
        exchange := Exchange,
        %exchange_type := ExchangeType,
        routing_key := RoutingKey,
        delivery_mode := DeliveryMode,
        wait_for_publish_confirmations := WaitForPublishConfirmations,
        publish_confirmation_timeout := PublishConfirmationTimeout
    } = Config,
    erlang:display({on_query, ResourceID, RequestType, Data, State}),
    try
        Method = #'basic.publish'{
            exchange = Exchange,
            routing_key = RoutingKey
        },
        erlang:display({xxx_delivery_mode, DeliveryMode}),
        AmqpMsg = #amqp_msg{
            props = #'P_basic'{headers = [], delivery_mode = DeliveryMode},
            payload = format_data(PayloadTemplate, Data)
        },
        ecpool:with_client(PoolName, fun(#{channel := Channel}) ->
            %% On query
            erlang:display({xxx_on_query, Channel, Method, AmqpMsg}),
            ok = amqp_channel:cast(Channel, Method, AmqpMsg),
            erlang:display({sent_xxx_on_query2, Channel, Method, AmqpMsg}),
            case WaitForPublishConfirmations of
                true -> true = amqp_channel:wait_for_confirms(Channel, PublishConfirmationTimeout);
                false -> ok
            end,
            erlang:display({xxx_done_wait_for_confirms})
        end)
    catch
        W:E:S ->
            erlang:display({on_query_error, W, E, S}),
            erlang:error({error, W, E, S})
    end.
% ?SLOG(debug, #{
%     msg => "clickhouse connector received sql query",
%     connector => ResourceID,
%     type => RequestType,
%     sql => DataOrSQL,
%     state => State
% }),
% %% Have we got a query or data to fit into an SQL template?
% SimplifiedRequestType = query_type(RequestType),
% #{templates := Templates} = State,
% SQL = get_sql(SimplifiedRequestType, Templates, DataOrSQL),
% ClickhouseResult = execute_sql_in_clickhouse_server(PoolName, SQL),
% transform_and_log_clickhouse_result(ClickhouseResult, ResourceID, SQL).

on_batch_query(
    _ResourceID,
    BatchReq,
    State
) ->
    try
        erlang:display({xxx_do_batch}),
        %% Currently we only support batch requests with the send_message key
        {Keys, MessagesToInsert} = lists:unzip(BatchReq),
        ensure_keys_are_of_type_send_message(Keys),
        %% Pick out the SQL template
        #{
            processed_payload_template := PayloadTemplate,
            poolname := PoolName,
            config := Config
        } = State,
        %% Create batch insert SQL statement
        FormattedMessages = [
            format_data(PayloadTemplate, Data)
         || Data <- MessagesToInsert
        ],
        ecpool:with_client(PoolName, fun(#{channel := Channel}) ->
            erlang:display({xxx_before}),
            publish_messages(Channel, Config, FormattedMessages),
            erlang:display({xxx_after})
        end)
    catch
        W:E:S ->
            erlang:display({on_query_error, W, E, S}),
            erlang:error({error, W, E, S})
    end,
    ok.

ensure_keys_are_of_type_send_message(Keys) ->
    case lists:all(fun is_send_message_atom/1, Keys) of
        true ->
            ok;
        false ->
            erlang:error(
                {unrecoverable_error,
                    <<"Unexpected type for batch message (Expected send_message)">>}
            )
    end.

publish_messages(
    Channel,
    #{
        delivery_mode := DeliveryMode,
        routing_key := RoutingKey,
        exchange := Exchange,
        wait_for_publish_confirmations := WaitForPublishConfirmations,
        publish_confirmation_timeout := PublishConfirmationTimeout
    } = _Config,
    Messages
) ->
    MessageProperties = #'P_basic'{headers = [], delivery_mode = DeliveryMode},
    [
        amqp_channel:cast(
            Channel,
            #'basic.publish'{
                exchange = Exchange,
                routing_key = RoutingKey
            },
            #amqp_msg{payload = Message, props = MessageProperties}
        )
     || Message <- Messages
    ],
    erlang:display({xxx_sent_batch}),
    case WaitForPublishConfirmations of
        true -> true = amqp_channel:wait_for_confirms(Channel, PublishConfirmationTimeout);
        false -> ok
    end,
    erlang:display({xxx_sent_batch_done}).

is_send_message_atom(send_message) ->
    true;
is_send_message_atom(_) ->
    false.

format_data([], Msg) ->
    emqx_utils_json:encode(Msg);
format_data(Tokens, Msg) ->
    erlang:display({format_data, Tokens, Msg}),
    emqx_plugin_libs_rule:proc_tmpl(Tokens, Msg).
