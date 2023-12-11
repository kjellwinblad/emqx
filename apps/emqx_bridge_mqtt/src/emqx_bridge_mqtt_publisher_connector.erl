%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_publisher_connector).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/logger.hrl").

-behaviour(emqx_resource).

-export([on_message_received/3]).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1
]).

-export([on_async_result/2]).

-define(HEALTH_CHECK_TIMEOUT, 1000).

%% ===================================================================
%% When use this bridge as a data source, ?MODULE:on_message_received will be called
%% if the bridge received msgs from the remote broker.
on_message_received(Msg, HookPoint, ResId) ->
    emqx_resource_metrics:received_inc(ResId),
    emqx_hooks:run(HookPoint, [Msg]).

%% ===================================================================
callback_mode() -> async_if_possible.

on_start(ResourceId, Conf) ->
    ?SLOG(info, #{
        msg => "starting_mqtt_connector",
        connector => ResourceId,
        config => emqx_utils:redact(Conf)
    }),
    case start_egress(ResourceId, Conf) of
        {ok, Result2} ->
            {ok, Result2#{installed_channels => #{}}};
        {error, Reason} ->
            {error, Reason}
    end.

start_egress(ResourceId, Conf) ->
    % NOTE
    % We are ignoring the user configuration here because there's currently no reliable way
    % to ensure proper session recovery according to the MQTT spec.
    ClientOpts0 = emqx_bridge_mqtt_common:mk_client_opts(ResourceId, "egress", Conf),
    ClientOpts = maps:put(clean_start, true, ClientOpts0),
    start_egress(ResourceId, Conf, ClientOpts).

start_egress(ResourceId, Conf, ClientOpts) ->
    PoolName = <<ResourceId/binary, ":egress">>,
    PoolSize = maps:get(pool_size, Conf),
    Options = [
        {name, PoolName},
        {pool_size, PoolSize},
        {client_opts, ClientOpts}
    ],
    ok = emqx_resource:allocate_resource(ResourceId, egress_pool_name, PoolName),
    case emqx_resource_pool:start(PoolName, emqx_bridge_mqtt_egress, Options) of
        ok ->
            {ok, #{
                egress_pool_name => PoolName
            }};
        {error, {start_pool_failed, _, Reason}} ->
            {error, Reason}
    end.

on_add_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId,
    ChannelConfig
) ->
    ChannelState = maps:get(parameters, ChannelConfig),
    NewInstalledChannels = maps:put(ChannelId, ChannelState, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_remove_channel(
    _InstId,
    #{
        installed_channels := InstalledChannels
    } = OldState,
    ChannelId
) ->
    NewInstalledChannels = maps:remove(ChannelId, InstalledChannels),
    %% Update state
    NewState = OldState#{installed_channels => NewInstalledChannels},
    {ok, NewState}.

on_get_channel_status(
    _ResId,
    ChannelId,
    #{
        installed_channels := Channels
    } = _State
) when is_map_key(ChannelId, Channels) ->
    connected.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_stop(ResourceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_mqtt_connector",
        connector => ResourceId
    }),
    Allocated = emqx_resource:get_allocated_resources(ResourceId),
    ok = stop_ingress(Allocated),
    ok = stop_egress(Allocated).

stop_ingress(#{ingress_pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName);
stop_ingress(#{}) ->
    ok.

stop_egress(#{egress_pool_name := PoolName}) ->
    emqx_resource_pool:stop(PoolName);
stop_egress(#{}) ->
    ok.

on_query(
    ResourceId,
    {ChannelId, Msg},
    #{egress_pool_name := PoolName} = State
) ->
    ?TRACE(
        "QUERY",
        "send_msg_to_remote_node",
        #{
            message => Msg,
            connector => ResourceId,
            channel_id => ChannelId
        }
    ),
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    handle_send_result(with_egress_client(PoolName, send, [Msg, ChannelConfig]));
on_query(ResourceId, {send_message, Msg}, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

on_query_async(
    ResourceId,
    {ChannelId, Msg},
    CallbackIn,
    #{egress_pool_name := PoolName} = State
) ->
    ?TRACE("QUERY", "async_send_msg_to_remote_node", #{message => Msg, connector => ResourceId}),
    Callback = {fun on_async_result/2, [CallbackIn]},
    Channels = maps:get(installed_channels, State),
    ChannelConfig = maps:get(ChannelId, Channels),
    Result = with_egress_client(PoolName, send_async, [Msg, Callback, ChannelConfig]),
    case Result of
        ok ->
            ok;
        {ok, Pid} when is_pid(Pid) ->
            {ok, Pid};
        {error, Reason} ->
            {error, classify_error(Reason)}
    end;
on_query_async(ResourceId, {send_message, Msg}, _Callback, #{}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        connector => ResourceId,
        message => Msg,
        reason => "Egress is not configured"
    }).

with_egress_client(ResourceId, Fun, Args) ->
    ecpool:pick_and_do(ResourceId, {emqx_bridge_mqtt_egress, Fun, Args}, no_handover).

on_async_result(Callback, Result) ->
    apply_callback_function(Callback, handle_send_result(Result)).

apply_callback_function(F, Result) when is_function(F) ->
    erlang:apply(F, [Result]);
apply_callback_function({F, A}, Result) when is_function(F), is_list(A) ->
    erlang:apply(F, A ++ [Result]);
apply_callback_function({M, F, A}, Result) when is_atom(M), is_atom(F), is_list(A) ->
    erlang:apply(M, F, A ++ [Result]).

handle_send_result(ok) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_SUCCESS}}) ->
    ok;
handle_send_result({ok, #{reason_code := ?RC_NO_MATCHING_SUBSCRIBERS}}) ->
    ok;
handle_send_result({ok, Reply}) ->
    {error, classify_reply(Reply)};
handle_send_result({error, Reason}) ->
    {error, classify_error(Reason)}.

classify_reply(Reply = #{reason_code := _}) ->
    {unrecoverable_error, Reply}.

classify_error(disconnected = Reason) ->
    {recoverable_error, Reason};
classify_error(ecpool_empty) ->
    {recoverable_error, disconnected};
classify_error({disconnected, _RC, _} = Reason) ->
    {recoverable_error, Reason};
classify_error({shutdown, _} = Reason) ->
    {recoverable_error, Reason};
classify_error(shutdown = Reason) ->
    {recoverable_error, Reason};
classify_error(Reason) ->
    {unrecoverable_error, Reason}.

on_get_status(_ResourceId, State) ->
    Pools = maps:to_list(maps:with([egress_pool_name], State)),
    Workers = [{Pool, Worker} || {Pool, PN} <- Pools, {_Name, Worker} <- ecpool:workers(PN)],
    try emqx_utils:pmap(fun get_status/1, Workers, ?HEALTH_CHECK_TIMEOUT) of
        Statuses ->
            combine_status(Statuses)
    catch
        exit:timeout ->
            connecting
    end.

get_status({Pool, Worker}) ->
    case ecpool_worker:client(Worker) of
        {ok, Client} when Pool == egress_pool_name ->
            emqx_bridge_mqtt_egress:status(Client);
        {error, _} ->
            disconnected
    end.

combine_status(Statuses) ->
    %% NOTE
    %% Natural order of statuses: [connected, connecting, disconnected]
    %% * `disconnected` wins over any other status
    %% * `connecting` wins over `connected`
    case lists:reverse(lists:usort(Statuses)) of
        [Status | _] ->
            Status;
        [] ->
            disconnected
    end.
