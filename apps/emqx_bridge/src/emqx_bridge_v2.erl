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
-module(emqx_bridge_v2).

-behaviour(emqx_config_handler).
% -behaviour(emqx_config_backup).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    load/0,
    lookup/1,
    is_bridge_v2_type/1,
    id/2,
    id/3,
    parse_id/1,
    send_message/4,
    bridge_v2_type_to_connector_type/1,
    is_bridge_v2_resource_id/1,
    extract_connector_id_from_bridge_v2_id/1
]).

-define(ROOT_KEY, bridges_v2).

load() ->
    Bridge_V2s = emqx:get_config([?ROOT_KEY], #{}),
    lists:foreach(
        fun({Type, NamedConf}) ->
            lists:foreach(
                fun({Name, Conf}) ->
                    instantiate_bridge_v2(
                        Type,
                        Name,
                        Conf
                    )
                end,
                maps:to_list(NamedConf)
            )
        end,
        maps:to_list(Bridge_V2s)
    ).

lookup(Id) ->
    {Type, Name} = parse_id(Id),
    lookup(Type, Name).

lookup(Type, Name) ->
    case emqx:get_config([?ROOT_KEY, Type, Name], not_found) of
        not_found ->
            {error, iolist_to_binary(io_lib:format("Bridge not found: ~p:~p", [Type, Name]))};
        Config ->
            Config
    end.

instantiate_bridge_v2(
    _BridgeType,
    _BridgeName,
    #{enable := false}
) ->
    ok;
instantiate_bridge_v2(
    BridgeV2Type,
    BridgeName,
    #{connector := ConnectorName} = Config
) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    BridgeV2Id = id(BridgeV2Type, BridgeName, ConnectorName),
    %% Create metrics for Bridge V2
    ok = emqx_resource:create_metrics(BridgeV2Id),
    %% We might need to create buffer workers for Bridge V2
    case get_query_mode(BridgeV2Type, Config) of
        %% the Bridge V2 has built-in buffer, so there is no need for resource workers
        simple_sync ->
            ok;
        simple_async ->
            ok;
        %% The Bridge V2 is a consumer Bridge V2, so there is no need for resource workers
        no_queries ->
            ok;
        _ ->
            %% start resource workers as the query type requires them
            ok = emqx_resource_buffer_worker_sup:start_workers(BridgeV2Id, CreationOpts)
    end.

get_query_mode(BridgeV2Type, Config) ->
    CreationOpts = emqx_resource:fetch_creation_opts(Config),
    ResourceType = emqx_bridge_resource:bridge_to_resource_type(BridgeV2Type),
    emqx_resource:query_mode(ResourceType, Config, CreationOpts).

% send_message(Id, Tag, Message) ->
%     Config = lookup(Id),
%     Connector = maps:get(connector, Config),
%     {Type, Name} = parse_id(Id),
%     ConnectorId = resource_id(Type, Name),

% send_message(BridgeId, Message) ->
%     {BridgeType, BridgeName} = parse_id(BridgeId),
%     ResId = resource_id(BridgeType, BridgeName),
%     send_message(BridgeType, BridgeName, ResId, Message, #{}).

% send_message(BridgeType, BridgeName, ResId, Message, QueryOpts0) ->
%     send_message(BridgeType, BridgeName, ResId, Message, QueryOpts0, ResId).

send_message(BridgeType, BridgeName, Message, QueryOpts0) ->
    case lookup(BridgeType, BridgeName) of
        #{enable := true} = Config ->
            do_send_msg_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config);
        #{enable := false} ->
            {error, bridge_stopped}
    end.

do_send_msg_with_enabled_config(BridgeType, BridgeName, Message, QueryOpts0, Config) ->
    BridgeV2Id = emqx_bridge_v2:id(BridgeType, BridgeName),
    ConnectorResourceId = emqx_bridge_v2:extract_connector_id_from_bridge_v2_id(BridgeV2Id),
    try emqx_resource_manager:maybe_install_bridge_v2(ConnectorResourceId, BridgeV2Id) of
        ok ->
            do_send_msg_after_bridge_v2_installed(
                BridgeType,
                BridgeName,
                BridgeV2Id,
                Message,
                QueryOpts0,
                Config
            )
    catch
        Error:Reason:Stack ->
            Msg = iolist_to_binary(
                io_lib:format(
                    "Failed to install bridge_v2 ~p in connector ~p: ~p",
                    [BridgeV2Id, ConnectorResourceId, Reason]
                )
            ),
            ?SLOG(error, #{
                msg => Msg,
                error => Error,
                reason => Reason,
                stacktrace => Stack
            }),
            {error, Reason}
    end.

do_send_msg_after_bridge_v2_installed(
    BridgeType, BridgeName, BridgeV2Id, Message, QueryOpts0, Config
) ->
    case lookup(BridgeType, BridgeName) of
        #{enable := true} = Config ->
            QueryMode = get_query_mode(BridgeType, Config),
            QueryOpts = maps:merge(emqx_bridge:query_opts(Config), QueryOpts0#{
                query_mode => QueryMode
            }),
            emqx_resource:query(BridgeV2Id, {BridgeV2Id, Message}, QueryOpts);
        #{enable := false} ->
            {error, bridge_stopped}
    end.

parse_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [Type, Name] ->
            {Type, Name};
        [<<"bridge_v2">>, Type, Name | _] ->
            {Type, Name};
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

id(BridgeType, BridgeName) ->
    case lookup(BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            id(BridgeType, BridgeName, ConnectorName);
        Error ->
            error(Error)
    end.

id(BridgeType, BridgeName, ConnectorName) ->
    ConnectorType = bin(bridge_v2_type_to_connector_type(BridgeType)),
    <<"bridge_v2:", (bin(BridgeType))/binary, ":", (bin(BridgeName))/binary, ":connector:",
        (bin(ConnectorType))/binary, ":", (bin(ConnectorName))/binary>>.

bridge_v2_type_to_connector_type(kafka) ->
    kafka.

is_bridge_v2_type(kafka) -> true;
is_bridge_v2_type(_) -> false.

is_bridge_v2_resource_id(<<"bridge_v2:", _/binary>>) -> true;
is_bridge_v2_resource_id(_) -> false.

extract_connector_id_from_bridge_v2_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [<<"bridge_v2">>, _Type, _Name, <<"connector">>, ConnectorType, ConnecorName] ->
            <<"connector:", ConnectorType/binary, ":", ConnecorName/binary>>;
        _X ->
            error({error, iolist_to_binary(io_lib:format("Invalid bridge V2 ID: ~p", [Id]))})
    end.

% connector_resource_id(BridgeV2Type, BridgeV2Name) ->
%     case lookup(BridgeV2Type, BridgeV2Name) of
%         #{connector := ConnectorName} ->
%             ConnectorType = bridge_v2_type_to_connector_type(BridgeV2Type),
%             ConnectorId = id(ConnectorType, ConnectorName),
%             <<"connector:", ConnectorId/binary>>;
%         Error ->
%             error(Error)
%     end.

% id(BridgeType, BridgeName) ->
%     Name = bin(BridgeName),
%     Type = bin(BridgeType),
%     <<Type/binary, ":", Name/binary>>.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
