%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_bridge_mqtt_ingress).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-behaviour(ecpool_worker).

%% ecpool
-export([connect/1]).

%% management APIs
-export([
    status/1,
    info/1
]).

-export([handle_publish/5]).
-export([handle_disconnect/1]).

-type name() :: term().

-type option() ::
    {name, name()}
    | {ingress, map()}
    %% see `emqtt:option()`
    | {client_opts, map()}.

-type ingress() :: #{
    server := string(),
    remote := #{
        topic := emqx_types:topic(),
        qos => emqx_types:qos()
    },
    local := emqx_bridge_mqtt_msg:msgvars(),
    on_message_received := {module(), atom(), [term()]}
}.

%% @doc Start an ingress bridge worker.
-spec connect([option() | {ecpool_worker_id, pos_integer()}]) ->
    {ok, pid()} | {error, _Reason}.
connect(Options) ->
    x:show(connect, Options),
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    x:show(worker_id, WorkerId),
    IsFirstWorker = WorkerId =:= 1,
    ?SLOG(debug, #{
        msg => "ingress_client_starting",
        options => emqx_utils:redact(Options)
    }),
    Name = proplists:get_value(name, Options),
    WorkerId = proplists:get_value(ecpool_worker_id, Options),
    IngressOptions = proplists:get_value(ingress, Options),
    PoolSize = maps:get(pool_size, IngressOptions),
    Ingress = config(IngressOptions, Name),
    ClientOpts = proplists:get_value(client_opts, Options),
    case
        emqtt:start_link(x:show(sub_with_opts, mk_client_opts(Name, WorkerId, Ingress, ClientOpts)))
    of
        {ok, Pid} ->
            connect(Pid, Name, Ingress, IsFirstWorker, PoolSize);
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_client_opts(Name, WorkerId, Ingress, ClientOpts = #{clientid := ClientId}) ->
    ClientOpts#{
        clientid := mk_clientid(WorkerId, ClientId),
        msg_handler => mk_client_event_handler(Name, Ingress)
    }.

mk_clientid(WorkerId, ClientId) ->
    iolist_to_binary([ClientId, $: | integer_to_list(WorkerId)]).

mk_client_event_handler(Name, Ingress = #{}) ->
    IngressVars = maps:with([server], Ingress),
    OnMessage = maps:get(on_message_received, Ingress, undefined),
    LocalPublish =
        case Ingress of
            #{local := Local = #{topic := _}} ->
                Local;
            #{} ->
                undefined
        end,
    #{
        publish => {fun ?MODULE:handle_publish/5, [Name, OnMessage, LocalPublish, IngressVars]},
        disconnected => {fun ?MODULE:handle_disconnect/1, []}
    }.

-spec connect(pid(), name(), ingress(), IsFirstWorker :: boolean(), PoolSize :: non_neg_integer()) ->
    {ok, pid()} | {error, _Reason}.
connect(Pid, Name, Ingress, IsFirstWorker, PoolSize) ->
    x:show(connect_3, Ingress),
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            IngressList = maps:get(ingress_list, Ingress, []),
            x:show(fucking_name, Name),
            %erlang:halt(),
            SubscribeResults = subscribe_remote_topics(
                Pid, IngressList, IsFirstWorker, PoolSize, Name
            ),
            %% Find error if any using proplists:get_value/2
            case proplists:get_value(error, SubscribeResults, ok) of
                ok ->
                    x:show(subscribe_connect),
                    {ok, Pid};
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "ingress_client_subscribe_failed",
                        ingress => Ingress,
                        name => Name,
                        reason => Reason
                    }),
                    _ = catch emqtt:stop(Pid),
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "ingress_client_connect_failed",
                reason => Reason,
                name => Name
            }),
            _ = catch emqtt:stop(Pid),
            Error
    end.

subscribe_remote_topics(Pid, IngressList, IsFirstWorker, PoolSize, Name) ->
    x:show(subscribe_remote_topics_2, IngressList),
    [subscribe_remote_topic(Pid, Ingress, IsFirstWorker, PoolSize, Name) || Ingress <- IngressList].

subscribe_remote_topic(
    Pid, #{remote := #{topic := RemoteTopic, qos := QoS}} = Remote, IsFirstWorker, PoolSize, Name
) ->
    case should_subscribe(RemoteTopic, IsFirstWorker, PoolSize, Name) of
        true ->
            x:show(subscribe_remote_topic_2, {RemoteTopic, QoS, Remote}),
            emqtt:subscribe(Pid, RemoteTopic, QoS);
        false ->
            ok
    end.

should_subscribe(RemoteTopic, IsFirstWorker, PoolSize, Name) ->
    case emqx_topic:parse(RemoteTopic) of
        {#share{} = _Filter, _SubOpts} ->
            % NOTE: this is shared subscription, many workers may subscribe
            true;
        {_Filter, #{}} when PoolSize > 1 ->
            % NOTE: this is regular subscription, only one worker should subscribe
            ?SLOG(warning, #{
                msg => "mqtt_pool_size_ignored",
                connector => Name,
                reason =>
                    "Remote topic filter is not a shared subscription, "
                    "only a single connection will be used from the connection pool",
                config_pool_size => PoolSize,
                pool_size => 1
            }),
            IsFirstWorker;
        {_Filter, #{}} when PoolSize == 1 ->
            % NOTE: this is regular subscription, only one worker should subscribe
            IsFirstWorker
    end.

%%

config(#{ingress_list := IngressList} = Conf, Name) ->
    NewIngressList = [fix_remote_config(Ingress, Name) || Ingress <- IngressList],
    Conf#{ingress_list => NewIngressList}.

-spec config(map(), name()) ->
    ingress().
fix_remote_config(#{remote := RC, local := LC} = Conf, BridgeName) ->
    Conf#{
        remote => parse_remote(RC, BridgeName),
        local => emqx_bridge_mqtt_msg:parse(LC)
    }.

parse_remote(#{qos := QoSIn} = Conf, BridgeName) ->
    QoS = downgrade_ingress_qos(QoSIn),
    case QoS of
        QoSIn ->
            ok;
        _ ->
            ?SLOG(warning, #{
                msg => "downgraded_unsupported_ingress_qos",
                qos_configured => QoSIn,
                qos_used => QoS,
                name => BridgeName
            })
    end,
    Conf#{qos => QoS}.

downgrade_ingress_qos(2) ->
    1;
downgrade_ingress_qos(QoS) ->
    QoS.

%%

-spec info(pid()) ->
    [{atom(), term()}].
info(Pid) ->
    emqtt:info(Pid).

-spec status(pid()) ->
    emqx_resource:resource_status().
status(Pid) ->
    try
        case proplists:get_value(socket, info(Pid)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
            disconnected
    end.

%%

handle_publish(#{properties := Props} = MsgIn, Name, OnMessage, LocalPublish, IngressVars) ->
    x:show(handle_publish_6, {MsgIn, Name, OnMessage, LocalPublish, IngressVars}),
    Msg = import_msg(MsgIn, IngressVars),
    ?SLOG(debug, #{
        msg => "ingress_publish_local",
        message => Msg,
        name => Name
    }),
    maybe_on_message_received(Msg, OnMessage),
    maybe_publish_local(Msg, LocalPublish, Props).

handle_disconnect(_Reason) ->
    ok.

maybe_on_message_received(Msg, {Mod, Func, Args}) ->
    x:show(maybe_on_message_received_2, {Msg, Mod, Func, Args}),
    erlang:apply(Mod, Func, [Msg | Args]);
maybe_on_message_received(_Msg, undefined) ->
    ok.

maybe_publish_local(Msg, Local = #{}, Props) ->
    emqx_broker:publish(to_broker_msg(Msg, Local, Props));
maybe_publish_local(_Msg, undefined, _Props) ->
    ok.

%%

import_msg(
    #{
        dup := Dup,
        payload := Payload,
        properties := Props,
        qos := QoS,
        retain := Retain,
        topic := Topic
    },
    #{server := Server}
) ->
    #{
        id => emqx_guid:to_hexstr(emqx_guid:gen()),
        server => to_bin(Server),
        payload => Payload,
        topic => Topic,
        qos => QoS,
        dup => Dup,
        retain => Retain,
        pub_props => printable_maps(Props),
        message_received_at => erlang:system_time(millisecond)
    }.

printable_maps(undefined) ->
    #{};
printable_maps(Headers) ->
    maps:fold(
        fun
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [
                        #{
                            key => Key,
                            value => Value
                        }
                     || {Key, Value} <- V0
                    ]
                };
            (K, V0, AccIn) ->
                AccIn#{K => V0}
        end,
        #{},
        Headers
    ).

%% published from remote node over a MQTT connection
to_broker_msg(Msg, Vars, undefined) ->
    to_broker_msg(Msg, Vars, #{});
to_broker_msg(#{dup := Dup} = Msg, Local, Props) ->
    #{
        topic := Topic,
        payload := Payload,
        qos := QoS,
        retain := Retain
    } = emqx_bridge_mqtt_msg:render(Msg, Local),
    PubProps = maps:get(pub_props, Msg, #{}),
    emqx_message:set_headers(
        Props#{properties => emqx_utils:pub_props_to_packet(PubProps)},
        emqx_message:set_flags(
            #{dup => Dup, retain => Retain},
            emqx_message:make(bridge, QoS, Topic, Payload)
        )
    ).

to_bin(B) when is_binary(B) -> B;
to_bin(Str) when is_list(Str) -> iolist_to_binary(Str).
