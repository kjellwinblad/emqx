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
    lookup/1,
    is_bridge_v2_type/1,
    resource_id/1,
    resource_id/2,
    id/2,
    create_buffer_workers_for_bridge_v2/2,
    send_message/6,
    bridge_v2_type_to_connector_type/1,
    connector_resource_id/2
]).

-define(ROOT_KEY, bridges_v2).

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

connector_resource_id(BridgeType, BridgeName) ->
    case lookup(BridgeType, BridgeName) of
        #{connector := ConnectorName} ->
            ConnectorType = bridge_v2_type_to_connector_type(BridgeType),
            emqx_connector_resource:resource_id(ConnectorType, ConnectorName);
        Error ->
            error(Error)
    end.

create_buffer_workers_for_bridge_v2(
    BridgeType,
    BridgeName
) ->
    case emqx:get_config([?ROOT_KEY, BridgeType, BridgeName], not_found) of
        not_found ->
            error({error, bridge_not_found});
        #{
            enable := true,
            %% TODO we should connect the connector id with the buffer workers somehow
            connector := _ConnectorName
        } = Config ->
            CreationOpts = emqx_resource:fetch_creation_opts(Config),
            BufferWorkerPoolId = resource_id(BridgeType, BridgeName),
            ok = emqx_resource_buffer_worker_sup:start_workers(BufferWorkerPoolId, CreationOpts);
        %% TODO: save the buffer worker pool id so that it can be deleted when the bridge 2 is deleted
        #{enable := false} ->
            error({error, bridge_disabled})
    end.

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

send_message(BridgeType, BridgeName, ConnectorResourceId, Message, QueryOpts0, MessageTag) ->
    case lookup(BridgeType, BridgeName) of
        #{enable := true} = Config ->
            QueryOpts = maps:merge(emqx_bridge:query_opts(Config), QueryOpts0),
            emqx_resource:query(ConnectorResourceId, {MessageTag, Message}, QueryOpts);
        #{enable := false} ->
            {error, bridge_stopped}
    end.

parse_id(Id) ->
    case binary:split(Id, <<":">>, [global]) of
        [Type, Name] ->
            {Type, Name};
        _ ->
            error({error, iolist_to_binary(io_lib:format("Invalid id: ~p", [Id]))})
    end.

resource_id(BridgeV2Id) when is_binary(BridgeV2Id) ->
    <<"bridge_2:", BridgeV2Id/binary>>.

resource_id(BridgeType, BridgeName) ->
    BridgeId = id(BridgeType, BridgeName),
    resource_id(BridgeId).

id(BridgeType, BridgeName) ->
    Name = bin(BridgeName),
    Type = bin(BridgeType),
    <<Type/binary, ":", Name/binary>>.

bridge_v2_type_to_connector_type(kafka) ->
    kafka.

is_bridge_v2_type(kafka) -> true;
is_bridge_v2_type(_) -> false.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).
