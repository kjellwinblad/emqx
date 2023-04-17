% %%--------------------------------------------------------------------
% %% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
% %%
% %% Licensed under the Apache License, Version 2.0 (the "License");
% %% you may not use this file except in compliance with the License.
% %% You may obtain a copy of the License at
% %% http://www.apache.org/licenses/LICENSE-2.0
% %%
% %% Unless required by applicable law or agreed to in writing, software
% %% distributed under the License is distributed on an "AS IS" BASIS,
% %% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% %% See the License for the specific language governing permissions and
% %% limitations under the License.
% %%--------------------------------------------------------------------

-module(emqx_ee_connector_rabbitmq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% This test SUITE requires a running RabbitMQ instance. If you don't want to
%% bring up the whole CI infrastuctucture with the `scripts/ct/run.sh` script
%% you can create a clickhouse instance with the following command.
%% 5672 is the default port for AMQP 0-9-1 and 15672 is the default port for
%% the HTTP managament interface.
%%
%% docker run -it --rm --name rabbitmq -p 127.0.0.1:5672:5672 -p 127.0.0.1:15672:15672 rabbitmq:3.11-management

rabbit_mq_host() ->
    <<"localhost">>.

rabbit_mq_port() ->
    5672.

rabbit_mq_exchange() ->
    <<"test_exchange">>.

rabbit_mq_queue() ->
    <<"test_queue">>.

rabbit_mq_routing_key() ->
    <<"test_routing_key">>.

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    erlang:display({xxx_enable_trace, application:ensure_started(recon)}),

    recon_trace:calls(
        {emqx_resource_manager, '_', '_'},
        100,
        %% {io_server, user}
        []
    ),
    case
        emqx_common_test_helpers:is_tcp_server_available(
            erlang:binary_to_list(rabbit_mq_host()), rabbit_mq_port()
        )
    of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            {ok, _} = application:ensure_all_started(emqx_ee_connector),
            {ok, _} = application:ensure_all_started(amqp_client),
            ChannelConnection = setup_rabbit_mq_exchange_and_queue(),
            [{channel_connection, ChannelConnection} | Config];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_rabbitmq);
                _ ->
                    {skip, no_rabbitmq}
            end
    end.

setup_rabbit_mq_exchange_and_queue() ->
    %% Create an exachange and a queue
    {ok, Connection} =
        amqp_connection:start(#amqp_params_network{
            host = erlang:binary_to_list(rabbit_mq_host()),
            port = rabbit_mq_port()
        }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    %% Create an exchange
    #'exchange.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'exchange.declare'{
                exchange = rabbit_mq_exchange(),
                type = <<"topic">>
            }
        ),
    %% Create a queue
    #'queue.declare_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.declare'{queue = rabbit_mq_queue()}
        ),
    %% Bind the queue to the exchange
    RoutingKey = rabbit_mq_routing_key(),
    #'queue.bind_ok'{} =
        amqp_channel:call(
            Channel,
            #'queue.bind'{
                queue = rabbit_mq_queue(),
                exchange = rabbit_mq_exchange(),
                routing_key = rabbit_mq_routing_key()
            }
        ),
    #{
        connection => Connection,
        channel => Channel
    }.

get_channel_connection(Config) ->
    proplists:get_value(channel_connection, Config).

end_per_suite(Config) ->
    #{
        connection := Connection,
        channel := Channel
    } = get_channel_connection(Config),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector),
    %% Close the channel
    ok = amqp_channel:close(Channel),
    %% Close the connection
    ok = amqp_connection:close(Connection).

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_ee_connector_rabbitmq_SUITE">>,
        rabbitmq_config()
    ).

show(X) ->
    erlang:display(X),
    X.

show(Label, What) ->
    erlang:display({Label, What}),
    What.

perform_lifecycle_check(PoolName, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(emqx_ee_connector_rabbitmq, InitialConfig),
    {ok, #{
        state := #{poolname := ReturnedPoolName} = State,
        status := InitialStatus
    }} =
        emqx_resource:create_local(
            PoolName,
            ?CONNECTOR_RESOURCE_GROUP,
            emqx_ee_connector_rabbitmq,
            CheckedConfig,
            #{}
        ),
    ?assertEqual(InitialStatus, connected),
    %% Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    %% Perform query as further check that the resource is working as expected
    (fun() ->
        erlang:display({pool_name, PoolName}),
        QueryNoParamsResWrapper = emqx_resource:query(PoolName, test_data()),
        erlang:display({query_no_params_res_wrapper, QueryNoParamsResWrapper})
    % ?assertMatch({ok, _}, QueryNoParamsResWrapper),
    % {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
    % ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
    end)().
% ?assertEqual(ok, emqx_resource:stop(PoolName)),
% % Resource will be listed still, but state will be changed and healthcheck will fail
% % as the worker no longer exists.
% {ok, ?CONNECTOR_RESOURCE_GROUP, #{
%     state := State,
%     status := StoppedStatus
% }} =
%     emqx_resource:get_instance(PoolName),
% ?assertEqual(stopped, StoppedStatus),
% ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(PoolName)),
% % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
% ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
% % Can call stop/1 again on an already stopped instance
% ?assertEqual(ok, emqx_resource:stop(PoolName)),
% % Make sure it can be restarted and the healthchecks and queries work properly
% ?assertEqual(ok, emqx_resource:restart(PoolName)),
% % async restart, need to wait resource
% timer:sleep(500),
% {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
%     emqx_resource:get_instance(PoolName),
% ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
% (fun() ->
%     QueryNoParamsResWrapper =
%         emqx_resource:query(PoolName, test_query_no_params()),
%     ?assertMatch({ok, _}, QueryNoParamsResWrapper),
%     {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
%     ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
% end)(),
% % Stop and remove the resource in one go.
% ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
% ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
% % Should not even be able to get the resource data out of ets now unlike just stopping.
% ?assertEqual({error, not_found}, emqx_resource:get_instance(PoolName)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

rabbitmq_config() ->
    Config =
        #{
            server => rabbit_mq_host(),
            port => 5672,
            exchange => rabbit_mq_exchange(),
            routing_key => rabbit_mq_routing_key()
        },
    #{<<"config">> => Config}.

test_data() ->
    {query, #{<<"msg_field">> => <<"Hello">>}}.
